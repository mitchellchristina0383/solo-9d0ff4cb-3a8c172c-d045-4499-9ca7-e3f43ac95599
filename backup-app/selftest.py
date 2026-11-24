"""端到端自测（无需 Docker，直接对应用逻辑 + Flask 测试客户端打）。

覆盖：
  T1 清零语义：缺省继承、显式清零、清零后重写“复活”
  T2 清退正确性：存活恢复点逐字节不变、编号不重排、无临时整盘
  T3 交错清退：同一旧链两笔任务，先发布生效、后发布 409 并可重试
  T4 中断重启：准备后“重启”，任务作废、仍服务最后发布的链，重发可成功
  T5 HTTP 冒烟：页面、下载 Content-Length、镜像哈希与真值一致、边界 409
  T6 选择性回退：两点比对、清零/照留语义、末块冲突、长度不符、比对途中清退
  T7 回退任务重启恢复：comparing 半成品与孤儿文件清理、ready 任务存活
"""
from __future__ import annotations

import hashlib
import os
import tempfile

from app import chain, revert
from app.db import connect, init_db
from app.seed import expected_image
from app.server import create_app

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def image_bytes(conn, backup_id: str) -> bytes:
    return b"".join(chain.materialize_blocks(conn, backup_id))


def images_before(conn) -> dict[str, bytes]:
    snap = chain.chain_snapshot(conn)
    return {b["id"]: image_bytes(conn, b["id"]) for b in snap["backups"]}


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="backup-selftest-")
    db_path = os.path.join(tmp, "test.db")
    print(f"自测数据库: {db_path}\n")

    # -------------------------------------------------------- T1 清零语义
    print("T1 清零/继承语义（种子链真值比对）")
    conn = connect(db_path)
    init_db(conn)
    from app.seed import seed_if_empty
    seed_if_empty(conn)

    snap = chain.chain_snapshot(conn)
    check("种子链有 5 个恢复点", len(snap["backups"]) == 5)
    check("链版本初始为 0", snap["chain_version"] == 0)
    check("块大小/块数正确", (snap["block_size"], snap["block_count"]) == (64, 8))

    bs = snap["block_size"]
    for b in snap["backups"]:
        got = image_bytes(conn, b["id"])
        want = expected_image(b["seq"])
        check(f"P{b['seq']} 还原镜像与真值一致", got == want,
              f"got {hashlib.sha256(got).hexdigest()[:12]} "
              f"want {hashlib.sha256(want).hexdigest()[:12]}")

    p4 = image_bytes(conn, next(b["id"] for b in snap["backups"] if b["seq"] == 4))
    check("P4 槽3 仍为显式零（P3 清零后无人改写）", p4[3*bs:4*bs] == b"\x00"*bs)
    check("P4 槽6 为显式零（P4 自己清零）", p4[6*bs:7*bs] == b"\x00"*bs)
    check("P4 槽1 清零后被 P4 重写复活（非零）", p4[1*bs:2*bs] != b"\x00"*bs)
    check("P4 槽0 继承到 P3 的 D", p4[0*bs:1*bs][:1] == b"D")
    check("P4 槽2 一路继承 P1 的 B", p4[2*bs:3*bs][:1] == b"B")

    # -------------------------------------------------------- T2 清退正确性
    print("\nT2 中间恢复点清退（P2 → 并入 P3）")
    before = images_before(conn)
    p2 = next(b["id"] for b in snap["backups"] if b["seq"] == 2)
    prep = chain.prepare_eviction(conn, p2)
    check("准备阶段返回 ready 任务", prep["state"] == "ready" and prep["merged_blocks"] == 2,
          str(prep))
    # 准备期间原链仍可逐点还原且内容不变
    mid = images_before(conn)
    check("准备期间原链 5 个恢复点全部仍可还原", len(mid) == 5)
    check("准备期间所有还原内容与准备前完全一致", mid == before)
    check("暂存期间被清退点仍在链上",
          any(b["id"] == p2 for b in chain.chain_snapshot(conn)["backups"]))

    pub = chain.publish_eviction(conn, prep["job_id"])
    check("发布后链版本 0 → 1", pub["new_version"] == 1)
    after_snap = chain.chain_snapshot(conn)
    surviving = after_snap["backups"]
    check("恢复点数量 5 → 4", len(surviving) == 4)
    seqs = [b["seq"] for b in surviving]
    check("编号保留原 seq、出现缺口且不重排", seqs == [0, 1, 3, 4], str(seqs))
    check("UUID 保持不变", set(before) - {p2} == {b["id"] for b in surviving})
    for b in surviving:
        got = image_bytes(conn, b["id"])
        check(f"存活点 P{b['seq']} 还原内容与清退前逐字节一致", got == before[b["id"]])
    check("已清退点不可再还原（404）",
          _expect_not_found(conn, p2))
    check("重复发布同一任务报冲突",
          _expect_conflict(lambda: chain.publish_eviction(conn, prep["job_id"])))
    check("块内容仍在 blocks 表（哈希共享，不随指针删除）",
          conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"] >= 6)

    # 数据目录里不应出现任何整盘镜像临时文件
    leftovers = [f for f in os.listdir(tmp) if f.endswith((".img", ".tmp"))]
    check("全程未生成临时整盘文件", leftovers == [], str(leftovers))

    # 再清退一个点验证可重复操作：P1 → 并入 P3（现在 P3 是 P1 的直接后继）
    print("\nT2b 第二次清退（P1 → 并入当前后继 P3），链版本再 +1")
    p1 = next(b["id"] for b in surviving if b["seq"] == 1)
    before2 = images_before(conn)
    prep2 = chain.prepare_eviction(conn, p1)
    pub2 = chain.publish_eviction(conn, prep2["job_id"])
    check("第二次发布后链版本 1 → 2", pub2["new_version"] == 2)
    s2 = chain.chain_snapshot(conn)
    check("恢复点数量 4 → 3，编号 [0,3,4]",
          [b["seq"] for b in s2["backups"]] == [0, 3, 4])
    for b in s2["backups"]:
        check(f"存活点 P{b['seq']} 内容仍与最初一致",
              image_bytes(conn, b["id"]) == before2[b["id"]])

    # -------------------------------------------------------- T3 交错清退
    print("\nT3 交错清退（同一旧链两笔任务的乐观并发）")
    p3 = next(b["id"] for b in s2["backups"] if b["seq"] == 3)
    job_a = chain.prepare_eviction(conn, p3)
    check("已有 ready 任务时再准备被拒绝（串行化）",
          _expect_conflict(lambda: chain.prepare_eviction(conn, p3)))
    # 模拟另一笔基于同一旧链（version=2）的任务：放弃 A 后准备 B，再让 A 仍旧发布
    job_b_payload = dict(job_a)  # 同一基线版本 2、同一清退对象的等价任务
    chain.abandon_job(conn, job_a["job_id"])
    job_b = chain.prepare_eviction(conn, p3)
    check("两任务基线版本相同（同一旧链）",
          job_b["based_on_version"] == job_b_payload["based_on_version"] == 2)
    pub_b = chain.publish_eviction(conn, job_b["job_id"])
    check("先发布的 B 生效，链版本 → 3", pub_b["new_version"] == 3)
    # 手工把 A 以相同基线重新插回，制造“基于同一旧链的迟到任务”
    stale_id = _insert_ready_job(conn, p3, based_on_version=2)
    err = _expect_conflict_msg(conn, stale_id)
    check("后发布的 A 报 409 并提示重试", err is not None and "重试" in err, err or "")
    check("失败任务被标记 stale",
          chain.get_job(conn, stale_id)["state"] == "stale")
    check("基于新链（v3）重新准备并发布可以成功",
          _retry_succeeds(conn))

    # -------------------------------------------------------- T4 中断重启
    print("\nT4 中断 / 重启（最后发布的链持久化，半成品任务作废）")
    # 用独立库，避免受前面清退结果影响
    restart_db = os.path.join(tmp, "restart.db")
    conn = connect(restart_db)
    init_db(conn)
    seed_if_empty(conn)
    # 先完成一笔已发布清退，制造“最后发布的链”（版本 1）
    p2_id = next(b["id"] for b in chain.chain_snapshot(conn)["backups"] if b["seq"] == 2)
    chain.publish_eviction(conn, chain.prepare_eviction(conn, p2_id)["job_id"])
    current = chain.chain_snapshot(conn)
    before3 = images_before(conn)
    p4_id = next(b["id"] for b in current["backups"] if b["seq"] == 4)
    p1_id = next(b["id"] for b in current["backups"] if b["seq"] == 1)
    job_c = chain.prepare_eviction(conn, p1_id)
    check("中断前暂存区已写入",
          conn.execute("SELECT COUNT(*) c FROM staged_blocks WHERE job_id=?",
                       (job_c["job_id"],)).fetchone()["c"] == job_c["merged_blocks"])

    # 模拟进程重启：重新连接、重新跑启动恢复
    conn2 = connect(restart_db)
    init_db(conn2)
    stale_n = chain.recover_pending_jobs(conn2)
    check("重启后 ready 任务被标记 stale", stale_n >= 1 and
          chain.get_job(conn2, job_c["job_id"])["state"] == "stale")
    restarted = chain.chain_snapshot(conn2)
    check("重启后服务的是最后发布的链（版本 1，编号 [0,1,3,4]）",
          restarted["chain_version"] == 1 and
          [b["seq"] for b in restarted["backups"]] == [0, 1, 3, 4])
    for b in restarted["backups"]:
        check(f"重启后 P{b['seq']} 还原内容不变",
              image_bytes(conn2, b["id"]) == before3[b["id"]])
    check("重启后不能直接发布旧任务",
          _expect_conflict(lambda: chain.publish_eviction(conn2, job_c["job_id"])))
    retry = chain.prepare_eviction(conn2, p1_id)
    again = chain.publish_eviction(conn2, retry["job_id"])
    check("重新发起清退可成功，链版本 → 2", again["new_version"] == 2)
    check("最终编号 [0,3,4]",
          [b["seq"] for b in chain.chain_snapshot(conn2)["backups"]] == [0, 3, 4])
    check("末端 P4 内容自始至终未变",
          image_bytes(conn2, p4_id) == before3[p4_id])
    conn2.close()
    conn.close()

    # -------------------------------------------------------- T5 HTTP 冒烟
    print("\nT5 HTTP 层冒烟（Flask 测试客户端 + 流式下载）")
    http_db = os.path.join(tmp, "http.db")
    app = create_app(http_db)
    client = app.test_client()
    r = client.get("/")
    check("GET / 返回页面", r.status_code == 200 and "备份链".encode() in r.data)
    r = client.get("/healthz")
    check("GET /healthz", r.status_code == 200 and r.get_json()["status"] == "ok")
    r = client.get("/api/chain")
    chain_json = r.get_json()
    check("GET /api/chain 返回 5 个恢复点", len(chain_json["backups"]) == 5)
    target = next(b for b in chain_json["backups"] if b["seq"] == 4)
    r = client.get(f"/backups/{target['id']}/download")
    check("下载状态 200 且 Content-Length=512",
          r.status_code == 200 and r.headers["Content-Length"] == "512")
    check("下载镜像哈希与真值一致",
          hashlib.sha256(r.data).digest() == hashlib.sha256(expected_image(4)).digest())
    check("下载文件名带编号", "P4" in r.headers["Content-Disposition"])
    r = client.get("/backups/nonexistent/download")
    check("不存在恢复点下载返回 404", r.status_code == 404)

    full = next(b for b in chain_json["backups"] if b["kind"] == "full")
    last = chain_json["backups"][-1]
    check("清退全备返回 409",
          client.post(f"/api/backups/{full['id']}/prepare").status_code == 409)
    check("清退末端恢复点返回 409",
          client.post(f"/api/backups/{last['id']}/prepare").status_code == 409)
    mid_id = chain_json["backups"][2]["id"]
    r = client.post(f"/api/backups/{mid_id}/prepare")
    check("合法清退准备返回 201", r.status_code == 201)
    jid = r.get_json()["job_id"]
    r = client.post(f"/api/jobs/{jid}/publish")
    check("HTTP 发布成功并版本 +1",
          r.status_code == 200 and r.get_json()["new_version"] == 1)

    # 应用工厂重启路径：再 create_app 一次，ready 任务应被自动清理
    app2 = create_app(http_db)
    with app2.test_client() as c2:
        check("工厂重启后无 ready 任务残留",
              all(j["state"] != "ready" for j in c2.get("/api/jobs").get_json()))
        check("工厂重启后链版本保持为 1",
              c2.get("/api/chain").get_json()["chain_version"] == 1)

    # -------------------------------------------------------- T6 选择性回退
    print("\nT6 选择性回退（两点比对 + 上传当前镜像 + 整盘核对）")
    rv_db = os.path.join(tmp, "revert.db")
    c = create_app(rv_db).test_client()
    snap = c.get("/api/chain").get_json()
    by_seq = {b["seq"]: b["id"] for b in snap["backups"]}
    bs = snap["block_size"]
    rdir = revert.revert_dir(rv_db)

    def rv_prepare(pre, post):
        return c.post("/api/revert/prepare",
                      json={"pre_id": by_seq[pre], "post_id": by_seq[post]})

    def rv_upload(rid, data):
        return c.post(f"/api/revert/{rid}/image", data=data,
                      content_type="application/octet-stream")

    # T6a 基本回退 + 清零语义：pre=P3, post=P4，当前盘 == P4 镜像
    r = rv_prepare(3, 4)
    check("回退准备返回 201", r.status_code == 201)
    prep = r.get_json()
    check("P3→P4 有 3 个变化块（槽1/6/7）", prep["changed_blocks"] == 3, str(prep))
    check("快照基于同一已提交链 v0", prep["based_on_version"] == 0)
    rid_a = prep["revert_id"]
    check("未上传时下载被拒绝（409）",
          c.get(f"/api/revert/{rid_a}/download").status_code == 409)
    r = rv_upload(rid_a, expected_image(4))
    check("比对通过返回 200", r.status_code == 200, r.data.decode())
    out = r.get_json()
    check("回退 3 块、照留 0 块", out["reverted"] == 3 and out["already_pre"] == 0, str(out))
    r = c.get(f"/api/revert/{rid_a}/download")
    check("回退镜像可下载且 Content-Length=512",
          r.status_code == 200 and r.headers["Content-Length"] == "512")
    check("回退结果 == P3 镜像（槽6 由零恢复为 A、槽7 回退为 A）",
          r.data == expected_image(3))
    check("回退后槽1 为全零（显式清零按实际字节回写）", r.data[bs:2*bs] == b"\x00"*bs)
    check("暂存目录恰有一份整盘文件",
          os.listdir(rdir) == [f"revert-{rid_a}.img"], str(os.listdir(rdir)))
    check("重复上传被拒绝（409）", rv_upload(rid_a, expected_image(4)).status_code == 409)

    # T6b 照留语义：当前值已等于故障前值 → 照留；两点间未变化的块 → 照留
    custom = b"X" * bs
    cur = bytearray(expected_image(4))
    cur[bs:2*bs] = b"\x00" * bs   # 槽1 当前值已等于故障前值（P3 时为零）→ 照留
    cur[2*bs:3*bs] = custom       # 槽2 两点间未变化 → 照留（保留当前值）
    rid_b = rv_prepare(3, 4).get_json()["revert_id"]
    r = rv_upload(rid_b, bytes(cur))
    out = r.get_json()
    check("照留场景：回退 2 块（6、7）、照留 1 块（1）",
          r.status_code == 200 and out["reverted"] == 2 and out["already_pre"] == 1,
          str(out))
    want = bytearray(expected_image(3))
    want[2*bs:3*bs] = custom
    r = c.get(f"/api/revert/{rid_b}/download")
    check("照留场景镜像：槽2 保留当前值，槽6/7 已回退", r.data == bytes(want))

    # T6c 末块冲突：当前值与两点都不同 → 整盘拒绝
    cur = bytearray(expected_image(4))
    cur[7*bs:8*bs] = b"R" * bs    # 末块（槽7）与 P3(A)、P4(E) 都不同
    rid_c = rv_prepare(3, 4).get_json()["revert_id"]
    r = rv_upload(rid_c, bytes(cur))
    check("末块冲突返回 409", r.status_code == 409, r.data.decode())
    body = r.get_json()
    check("冲突块号列出 [7]", body.get("conflicts") == [7], str(body))
    job = c.get(f"/api/revert/{rid_c}").get_json()
    check("任务终态 failed 且记录冲突块", job["state"] == "failed" and job["conflicts"] == [7])
    check("冲突任务下载被拒绝（409）",
          c.get(f"/api/revert/{rid_c}/download").status_code == 409)
    leftovers = [f for f in os.listdir(rdir) if rid_c in f]
    check("冲突后暂存文件已删除", leftovers == [], str(leftovers))

    # T6d 长度不符：短/长都拒绝
    rid_d = rv_prepare(3, 4).get_json()["revert_id"]
    r = rv_upload(rid_d, b"\x00" * 100)
    check("短镜像返回 400 且说明长度", r.status_code == 400 and "长度" in r.get_json()["message"])
    rid_d2 = rv_prepare(3, 4).get_json()["revert_id"]
    r = rv_upload(rid_d2, expected_image(4) + b"x" * 10)
    check("长镜像返回 400", r.status_code == 400)
    check("长度不符的任务为 failed 且无暂存文件残留",
          c.get(f"/api/revert/{rid_d}").get_json()["state"] == "failed"
          and not [f for f in os.listdir(rdir) if rid_d in f or rid_d2 in f])

    # T6e 比对途中清退：选中的故障前点 P1 随后被清退，比对仍可完成
    prep_e = rv_prepare(1, 3).get_json()
    rid_e = prep_e["revert_id"]
    check("P1→P3 有 4 个变化块（槽0/1/3/5）", prep_e["changed_blocks"] == 4)
    r = c.post(f"/api/backups/{by_seq[1]}/prepare")
    job_e = r.get_json()["job_id"]
    r = c.post(f"/api/jobs/{job_e}/publish")
    check("清退照常发布（选中点 P1 被清退，链版本 → 1）",
          r.status_code == 200 and r.get_json()["new_version"] == 1)
    check("P1 已从链上消失",
          [b["seq"] for b in c.get("/api/chain").get_json()["backups"]] == [0, 2, 3, 4])
    r = rv_upload(rid_e, expected_image(3))
    check("清退后比对仍可完成并通过", r.status_code == 200, r.data.decode())
    check("回退 4 块", r.get_json()["reverted"] == 4)
    r = c.get(f"/api/revert/{rid_e}/download")
    check("回退结果 == P1 镜像（被清退点的内容仍正确）", r.data == expected_image(1))

    # T6f 参数校验与任务列表/丢弃
    check("同一恢复点被拒绝（400）",
          c.post("/api/revert/prepare",
                 json={"pre_id": by_seq[2], "post_id": by_seq[2]}).status_code == 400)
    check("故障前晚于故障后被拒绝（400）", rv_prepare(4, 2).status_code == 400)
    check("不存在的恢复点返回 404",
          c.post("/api/revert/prepare",
                 json={"pre_id": "nope", "post_id": by_seq[2]}).status_code == 404)
    check("缺参数返回 400",
          c.post("/api/revert/prepare", json={"pre_id": by_seq[2]}).status_code == 400)
    listed = c.get("/api/revert").get_json()
    check("任务列表包含全部任务", len(listed) == 6, str(len(listed)))
    r = c.post(f"/api/revert/{rid_a}/discard")
    check("丢弃任务后暂存文件被删除",
          r.status_code == 200
          and not os.path.exists(os.path.join(rdir, f"revert-{rid_a}.img")))
    check("丢弃不存在的任务返回 404", c.post("/api/revert/nope/discard").status_code == 404)

    # -------------------------------------------------------- T7 回退任务重启恢复
    print("\nT7 回退任务的重启恢复（comparing 半成品 / 孤儿文件 / ready 存活）")
    rs_db = os.path.join(tmp, "revert-restart.db")
    cx = create_app(rs_db).test_client()
    by_seq2 = {b["seq"]: b["id"] for b in cx.get("/api/chain").get_json()["backups"]}
    rdir2 = revert.revert_dir(rs_db)
    rid_ok = cx.post("/api/revert/prepare",
                     json={"pre_id": by_seq2[3], "post_id": by_seq2[4]}).get_json()["revert_id"]
    cx.post(f"/api/revert/{rid_ok}/image", data=expected_image(4),
            content_type="application/octet-stream")
    check("重启前任务已 ready",
          cx.get(f"/api/revert/{rid_ok}").get_json()["state"] == "ready")
    # 手工制造一笔 comparing 半成品（含半截文件）与一个孤儿文件
    rid_half = cx.post("/api/revert/prepare",
                       json={"pre_id": by_seq2[2], "post_id": by_seq2[4]}).get_json()["revert_id"]
    half = os.path.join(rdir2, f"revert-{rid_half}.img")
    with open(half, "wb") as f:
        f.write(b"\x00" * 100)
    orphan = os.path.join(rdir2, "revert-orphan.img")
    with open(orphan, "wb") as f:
        f.write(b"\x00" * 10)
    conn_x = connect(rs_db)
    conn_x.execute(
        "UPDATE revert_jobs SET state = 'comparing', image_path = ? WHERE id = ?",
        (half, rid_half))
    conn_x.commit()
    conn_x.close()
    # 模拟重启：重新 create_app → 启动恢复
    app_rs2 = create_app(rs_db)
    check("启动恢复清理了半成品与孤儿文件（2 项）",
          app_rs2.config["REVERT_CLEANED_ON_START"] == 2,
          str(app_rs2.config["REVERT_CLEANED_ON_START"]))
    cx2 = app_rs2.test_client()
    check("comparing 半成品被标记 failed",
          cx2.get(f"/api/revert/{rid_half}").get_json()["state"] == "failed")
    check("半成品与孤儿文件已删除", not os.path.exists(half) and not os.path.exists(orphan))
    r = cx2.get(f"/api/revert/{rid_ok}/download")
    check("ready 任务在重启后仍可下载且内容正确",
          r.status_code == 200 and r.data == expected_image(3))

    conn.close()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


def _expect_not_found(conn, backup_id) -> bool:
    try:
        chain.get_backup(conn, backup_id)
        return False
    except chain.NotFoundError:
        return True


def _expect_conflict(fn) -> bool:
    try:
        fn()
        return False
    except chain.ConflictError:
        return True


def _expect_conflict_msg(conn, job_id):
    try:
        chain.publish_eviction(conn, job_id)
        return None
    except chain.ConflictError as e:
        return str(e)


def _insert_ready_job(conn, evict_id, based_on_version) -> str:
    from uuid import uuid4
    from app.chain import _now
    job_id = uuid4().hex
    conn.execute(
        "INSERT INTO merge_jobs(id, evict_id, based_on_version, state, created_at) "
        "VALUES(?, ?, ?, 'ready', ?)",
        (job_id, evict_id, based_on_version, _now()),
    )
    conn.commit()
    return job_id


def _retry_succeeds(conn) -> bool:
    snap = chain.chain_snapshot(conn)
    if len(snap["backups"]) < 3:
        return True
    target = snap["backups"][1]["id"]
    try:
        job = chain.prepare_eviction(conn, target)
        out = chain.publish_eviction(conn, job["job_id"])
        return out["state"] == "published"
    except chain.ConflictError:
        return False


if __name__ == "__main__":
    raise SystemExit(main())
