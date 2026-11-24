"""SQLite 连接与建表。

库内同时保存：
  * 定长块的唯一内容（blocks）
  * 恢复点（backups）与恢复点到块的指针（backup_blocks）
  * 清退合并任务及其暂存区（merge_jobs / staged_blocks）
  * 选择性回退任务及其两点间变化槽位快照（revert_jobs / revert_blocks）

块内容只存一份，多个恢复点通过指针共享；清退中间恢复点时只改指针，
不复制块内容，也不会在盘上生成临时整盘镜像。
"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backups (
    id          TEXT PRIMARY KEY,           -- UUID，终身不变
    seq         INTEGER NOT NULL UNIQUE,    -- 显示编号，清退后出现缺口，不重排
    kind        TEXT NOT NULL CHECK (kind IN ('full', 'inc')),
    label       TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
    hash       TEXT PRIMARY KEY,            -- 内容哈希，去重用
    size       INTEGER NOT NULL,
    is_zero    INTEGER NOT NULL,            -- 1 表示“显式清零标记块”
    content    BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS backup_blocks (
    backup_id   TEXT NOT NULL REFERENCES backups(id),
    block_index INTEGER NOT NULL,
    block_hash  TEXT NOT NULL REFERENCES blocks(hash),
    is_zero     INTEGER NOT NULL,           -- 该恢复点在此槽位是否显式清零
    PRIMARY KEY (backup_id, block_index)
);

CREATE TABLE IF NOT EXISTS merge_jobs (
    id              TEXT PRIMARY KEY,       -- UUID
    evict_id        TEXT NOT NULL,          -- 待清退的中间恢复点
    based_on_version INTEGER NOT NULL,      -- 准备时依据的链版本（乐观并发）
    state           TEXT NOT NULL CHECK (state IN ('ready', 'published', 'stale')),
    created_at      TEXT NOT NULL,
    published_at    TEXT
);

-- 发布合并用的暂存区：只记录指针层改动，原链在发布前完全不动
CREATE TABLE IF NOT EXISTS staged_blocks (
    job_id      TEXT NOT NULL REFERENCES merge_jobs(id),
    block_index INTEGER NOT NULL,
    block_hash  TEXT NOT NULL REFERENCES blocks(hash),
    is_zero     INTEGER NOT NULL,
    PRIMARY KEY (job_id, block_index)
);

-- 选择性回退任务。pre_id/post_id 故意不设外键：清退会删除 backups 行，
-- 而“选中的点随后被清退，本次比对仍可完成”要求任务行不被牵连。
CREATE TABLE IF NOT EXISTS revert_jobs (
    id               TEXT PRIMARY KEY,      -- UUID
    pre_id           TEXT NOT NULL,         -- 故障前恢复点（快照时的 id/seq 留档）
    pre_seq          INTEGER NOT NULL,
    post_id          TEXT NOT NULL,         -- 故障后恢复点
    post_seq         INTEGER NOT NULL,
    based_on_version INTEGER NOT NULL,      -- 快照所在的链版本（同一已提交链）
    state            TEXT NOT NULL CHECK (state IN
                         ('awaiting_upload', 'comparing', 'ready', 'failed')),
    image_path       TEXT,                  -- 暂存/成品镜像（上传即回写对象，全程只此一份整盘文件）
    changed_blocks   INTEGER NOT NULL,      -- 两点间有变化的槽位数（快照时确定）
    reverted         INTEGER,               -- 实际回退块数
    already_pre      INTEGER,               -- 当前值已等于故障前值、照留的块数
    conflicts        TEXT,                  -- JSON 数组：与两点都不同的冲突块号
    error            TEXT,                  -- 失败原因（长度不符 / 中断等）
    created_at       TEXT NOT NULL,
    finished_at      TEXT
);

-- 回退快照：只存“两点间有变化”的槽位指针（故障前/后各一份），不存字节。
-- 块内容哈希共享、清退不删内容，所以快照在清退后依然可解析成实际字节。
CREATE TABLE IF NOT EXISTS revert_blocks (
    job_id      TEXT NOT NULL REFERENCES revert_jobs(id),
    block_index INTEGER NOT NULL,
    pre_hash    TEXT NOT NULL,
    pre_zero    INTEGER NOT NULL,
    post_hash   TEXT NOT NULL,
    post_zero   INTEGER NOT NULL,
    PRIMARY KEY (job_id, block_index)
);

CREATE INDEX IF NOT EXISTS idx_bb_lookup
    ON backup_blocks (backup_id, block_index);
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    # 外键打开；WAL 让“读原链”和“写暂存区”互不阻塞
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
