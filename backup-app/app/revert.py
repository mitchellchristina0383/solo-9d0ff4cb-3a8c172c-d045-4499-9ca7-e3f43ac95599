"""选择性回退：撤掉两点之间的误写，同时保留当前盘上后来写到其他块的数据。

流程（两阶段）
------------
1. ``prepare_revert``：在**同一份已提交链**的一致性读事务里，把故障前/后
   两个恢复点沿链折叠（继承 + 清零）成最终指针，只把“两点间有变化”的槽位
   快照进 ``revert_blocks``（指针级，不含字节）。
2. ``upload_and_compare``：流式接收当前 RAW 镜像落**一份**暂存文件，逐块
   读取比对并**就地回写**这同一份文件；上传 + 回退合计最多暂存一份整盘
   大小的文件数据，不整盘载入内存。

比对规则（按继承和清零后的实际字节）
------------------------------------
* 两点间有变化、且当前值仍等于故障后值 → 回写故障前字节（回退）；
* 两点间有变化、但当前值已等于故障前值 → 照留（视为已人工修好）；
* 两点间未变化的槽位 → 不比对、一律照留；
* 两点间有变化、当前值与两点都不同 → 冲突，列出块号，整盘拒绝。

清退兼容
--------
快照只依赖 ``blocks`` 表的内容（哈希共享，清退只删指针与恢复点行、不删块
内容），且 ``revert_jobs`` 不对 ``backups`` 设外键；因此清退照常发布，
即使选中的点随后被清退，本次比对仍可完成。
"""
from __future__ import annotations

import json
import os
import sqlite3
from uuid import uuid4

from . import chain

READ_CHUNK = 1 << 16  # 上传流式落盘的读块大小（64KiB，与内存占用上界）


# ------------------------------------------------------------- 路径/行 → dict
def revert_dir(db_path: str) -> str:
    """回退暂存目录：与数据库同级的 revert/（Docker 下即命名卷 /data/revert）。"""
    return os.path.join(os.path.dirname(os.path.abspath(db_path)), "revert")


def get_job(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM revert_jobs WHERE id = ?", (job_id,)).fetchone()
    if row is None:
        raise chain.NotFoundError(f"回退任务不存在: {job_id}")
    return row


def public(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "pre_id": row["pre_id"],
        "pre_seq": row["pre_seq"],
        "post_id": row["post_id"],
        "post_seq": row["post_seq"],
        "based_on_version": row["based_on_version"],
        "state": row["state"],
        "changed_blocks": row["changed_blocks"],
        "reverted": row["reverted"],
        "already_pre": row["already_pre"],
        "conflicts": json.loads(row["conflicts"]) if row["conflicts"] else None,
        "error": row["error"],
        "created_at": row["created_at"],
        "finished_at": row["finished_at"],
    }


def list_jobs(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM revert_jobs ORDER BY created_at DESC, id")
    return [public(r) for r in rows]


# ------------------------------------------------------------------ 第一阶段
def prepare_revert(conn: sqlite3.Connection, pre_id: str, post_id: str) -> dict:
    """在同一已提交链上快照两点间有变化的槽位指针。

    显式读事务保证链版本、两点指针取自同一份已提交链；快照落库后，
    清退发布与否都不再影响本次比对。
    """
    if not pre_id or not post_id:
        raise chain.DomainError("必须选择故障前、故障后两个恢复点")
    if pre_id == post_id:
        raise chain.DomainError("故障前、故障后不能是同一个恢复点")

    # 一致性读：版本与两点指针来自同一份已提交链（调用方已开事务则沿用）
    if not conn.in_transaction:
        conn.execute("BEGIN")
    try:
        version = chain.chain_version(conn)
        pre = chain.get_backup(conn, pre_id)    # 不存在 → 404
        post = chain.get_backup(conn, post_id)  # 不存在 → 404
        if pre["seq"] >= post["seq"]:
            raise chain.DomainError(
                f"故障前恢复点 P{pre['seq']} 必须早于故障后恢复点 P{post['seq']}"
            )
        pre_map = chain.resolve_pointers(conn, pre_id)
        post_map = chain.resolve_pointers(conn, post_id)
        _block_size, block_count = chain.disk_spec(conn)

        # 指针（哈希 + 清零标记）不同即实际字节不同：内容按哈希去重存储，
        # 同哈希必同字节，异哈希必异字节，与“按实际字节比较”口径一致。
        changed = [
            (i, *pre_map.get(i, (chain.ZERO_HASH, 1)), *post_map.get(i, (chain.ZERO_HASH, 1)))
            for i in range(block_count)
            if pre_map.get(i) != post_map.get(i)
        ]

        job_id = uuid4().hex
        conn.execute(
            "INSERT INTO revert_jobs(id, pre_id, pre_seq, post_id, post_seq, "
            "based_on_version, state, changed_blocks, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, 'awaiting_upload', ?, ?)",
            (
                job_id, pre_id, pre["seq"], post_id, post["seq"],
                version, len(changed), chain._now(),
            ),
        )
        conn.executemany(
            "INSERT INTO revert_blocks(job_id, block_index, "
            "pre_hash, pre_zero, post_hash, post_zero) VALUES(?, ?, ?, ?, ?, ?)",
            [(job_id, idx, pre_h, pre_z, post_h, post_z) for idx, pre_h, pre_z, post_h, post_z in changed],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "revert_id": job_id,
        "pre_seq": pre["seq"],
        "post_seq": post["seq"],
        "based_on_version": version,
        "changed_blocks": len(changed),
        "state": "awaiting_upload",
    }


# ------------------------------------------------------------------ 第二阶段
def _block_bytes(conn: sqlite3.Connection, block_hash: str, is_zero: int,
                 block_size: int, zero_block: bytes) -> bytes:
    """把槽位指针解析成实际字节（显式清零 → 全零）。"""
    if is_zero:
        return zero_block
    return chain._block_content(conn, block_hash)


def upload_and_compare(conn: sqlite3.Connection, db_path: str, job_id: str,
                       stream) -> dict:
    """接收当前 RAW 镜像 → 逐块比对 → 就地回写 → 整盘核对通过才放行下载。

    任何失败都会删掉暂存文件并把任务置为 failed（终态，可重新发起任务）。
    """
    job = get_job(conn, job_id)
    if job["state"] != "awaiting_upload":
        raise chain.ConflictError(f"任务状态为 {job['state']}，不能接收镜像")

    block_size, block_count = chain.disk_spec(conn)
    expected = block_size * block_count
    rdir = revert_dir(db_path)
    os.makedirs(rdir, exist_ok=True)
    path = os.path.join(rdir, f"revert-{job_id}.img")

    # 1) 流式落盘：最多写 expected+1 字节即停，超长立即判定长度不符
    written = 0
    with open(path, "wb") as f:
        while True:
            chunk = stream.read(READ_CHUNK)
            if not chunk:
                break
            f.write(chunk)
            written += len(chunk)
            if written > expected:
                break

    if written != expected:
        os.unlink(path)
        _fail(conn, job_id, f"镜像长度不符：期望 {expected} 字节，实收 {written} 字节")
        raise chain.DomainError(
            f"镜像长度不符：期望 {expected} 字节（{block_count} 块 × {block_size}B），"
            f"实收 {written} 字节",
            expected=expected, got=written,
        )

    # 2) 进入比对态（此刻崩溃/重启可由 recover_revert_jobs 识别并清理）
    conn.execute(
        "UPDATE revert_jobs SET state = 'comparing', image_path = ? WHERE id = ?",
        (path, job_id),
    )
    conn.commit()

    # 3) 逐块比对：只读“两点间有变化”的槽位，就地回写同一份文件
    changed = conn.execute(
        "SELECT block_index, pre_hash, pre_zero, post_hash, post_zero "
        "FROM revert_blocks WHERE job_id = ? ORDER BY block_index",
        (job_id,),
    ).fetchall()
    zero_block = b"\x00" * block_size
    reverted = 0
    already_pre = 0
    conflicts: list[int] = []

    with open(path, "r+b") as f:
        for r in changed:
            idx = r["block_index"]
            f.seek(idx * block_size)
            cur = f.read(block_size)
            pre = _block_bytes(conn, r["pre_hash"], r["pre_zero"], block_size, zero_block)
            post = _block_bytes(conn, r["post_hash"], r["post_zero"], block_size, zero_block)
            if cur == post:
                f.seek(idx * block_size)
                f.write(pre)  # 回退：换回故障前字节
                reverted += 1
            elif cur == pre:
                already_pre += 1  # 当前值已等于故障前值：照留
            else:
                conflicts.append(idx)  # 与两点都不同：冲突

    # 4) 整盘核对：有冲突即拒绝，删暂存文件，绝不提供下载
    if conflicts:
        os.unlink(path)
        _fail(conn, job_id, f"{len(conflicts)} 个块与两点都不同", conflicts)
        raise chain.ConflictError(
            f"块 {conflicts} 的当前值与故障前、故障后两点都不同，"
            "无法安全回退，已整盘拒绝",
            conflicts=conflicts,
        )

    conn.execute(
        "UPDATE revert_jobs SET state = 'ready', reverted = ?, already_pre = ?, "
        "finished_at = ? WHERE id = ?",
        (reverted, already_pre, chain._now(), job_id),
    )
    conn.commit()
    return {
        "revert_id": job_id,
        "state": "ready",
        "changed_blocks": len(changed),
        "reverted": reverted,
        "already_pre": already_pre,
        "conflicts": [],
    }


def _fail(conn: sqlite3.Connection, job_id: str, error: str,
          conflicts: list[int] | None = None) -> None:
    conn.execute(
        "UPDATE revert_jobs SET state = 'failed', error = ?, conflicts = ?, "
        "finished_at = ? WHERE id = ?",
        (error, json.dumps(conflicts) if conflicts else None, chain._now(), job_id),
    )
    conn.commit()


# ------------------------------------------------------------------ 下载/清理
def download_file(conn: sqlite3.Connection, job_id: str) -> tuple[str, str]:
    """整盘核对通过（ready）才返回 (文件路径, 下载文件名)。"""
    job = get_job(conn, job_id)
    if job["state"] != "ready":
        raise chain.ConflictError(
            f"任务状态为 {job['state']}，只有比对通过的任务才能下载"
        )
    path = job["image_path"]
    if not path or not os.path.exists(path):
        raise chain.ConflictError("回退镜像文件已不存在，请重新发起回退")
    filename = f"revert-P{job['pre_seq']}-P{job['post_seq']}.img"
    return path, filename


def discard_job(conn: sqlite3.Connection, job_id: str) -> dict:
    """丢弃任务：删除暂存/成品镜像与快照，释放暂存空间。"""
    job = get_job(conn, job_id)
    path = job["image_path"]
    if path and os.path.exists(path):
        os.unlink(path)
    conn.execute("DELETE FROM revert_blocks WHERE job_id = ?", (job_id,))
    conn.execute("DELETE FROM revert_jobs WHERE id = ?", (job_id,))
    conn.commit()
    return {"revert_id": job_id, "state": "discarded"}


def recover_revert_jobs(conn: sqlite3.Connection, db_path: str) -> int:
    """进程重启后调用：比对途中（comparing）的任务一律失败并清理暂存文件；
    ready 但文件丢失的标记失败；目录里无主的 revert-*.img 一律删除。"""
    cleaned = 0
    for job in conn.execute(
        "SELECT id, image_path FROM revert_jobs WHERE state = 'comparing'"
    ).fetchall():
        if job["image_path"] and os.path.exists(job["image_path"]):
            os.unlink(job["image_path"])
        _fail(conn, job["id"], "进程重启，比对中断")
        cleaned += 1

    for job in conn.execute(
        "SELECT id, image_path FROM revert_jobs WHERE state = 'ready'"
    ).fetchall():
        if not job["image_path"] or not os.path.exists(job["image_path"]):
            _fail(conn, job["id"], "暂存镜像丢失")
            cleaned += 1

    known = {
        r["image_path"]
        for r in conn.execute(
            "SELECT image_path FROM revert_jobs WHERE image_path IS NOT NULL"
        )
    }
    rdir = revert_dir(db_path)
    if os.path.isdir(rdir):
        for name in os.listdir(rdir):
            full = os.path.join(rdir, name)
            if name.startswith("revert-") and full not in known:
                os.unlink(full)
                cleaned += 1
    conn.commit()
    return cleaned
