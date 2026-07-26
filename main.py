"""
astrbot_plugin_grep_history_context
====================================
Copyright (C) 2026 依轨泠QTY (Qtgcy08)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

提供 grepHistoryContext 工具，供 LLM 搜索本 AstrBot 实例内所有对话上下文。

V3 改进（2026-07-26）：
- 双 FTS5 索引：conversations_fts（对话上下文）+ pmh_fts（平台消息历史）
- 混合检索：两个 FTS 表同时搜索
- 自动去重：pmh 通过 (platform_id, user_id) 映射回 conversation_id 后合并
- 覆盖被压缩/截断的旧对话数据
"""

import json
import os
import re
import sqlite3
import threading
from pathlib import Path
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api import AstrBotConfig

_FTS_LOCK = threading.Lock()


def _read_plugin_version() -> str:
    """从 metadata.yaml 中读取版本号，单一事实来源。"""
    meta_path = Path(__file__).parent / "metadata.yaml"
    try:
        text = meta_path.read_text(encoding="utf-8")
        m = re.search(r"^version:\s*(.+)$", text, re.MULTILINE)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return "0.0.0"


@register(
    "astrbot_plugin_grep_history_context",
    "闻人墨",
    "提供 grepHistoryContext 工具，搜索本 AstrBot 实例内所有对话上下文。",
    _read_plugin_version(),
)
class GrepHistoryContextPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.admin_only = config.get("admin_only", True)
        self._plugin_db_path: str | None = None
        self._core_db_path: str | None = None
        logger.info(
            f"GrepHistoryContextPlugin v3 initialized. admin_only={self.admin_only}"
        )

    # ──────────────── 路径管理 ────────────────

    def _get_plugin_db_path(self) -> str:
        """插件自有数据库路径（从 conversation_manager 获取 data 目录）。"""
        if self._plugin_db_path:
            return self._plugin_db_path
        plugin_data_dir = os.path.join(
            os.path.dirname(self.context.conversation_manager.db.db_path),
            "plugin_data",
            "astrbot_plugin_grep_history_context",
        )
        os.makedirs(plugin_data_dir, exist_ok=True)
        self._plugin_db_path = os.path.join(plugin_data_dir, "fts_index.db")
        return self._plugin_db_path

    def _get_core_db_path(self) -> str:
        """核心数据库路径（从 conversation_manager 获取，只读访问）。"""
        if self._core_db_path:
            return self._core_db_path
        self._core_db_path = self.context.conversation_manager.db.db_path
        return self._core_db_path

    # ──────────────── FTS5 索引管理 ────────────────

    def _ensure_fts_index(self):
        """每次搜索前检查增量，近实时同步两个 FTS5 索引（线程安全，幂等）。

        两条索引线：
        - conversations_fts：索引 conversations.content（对话上下文）
        - pmh_fts：索引 platform_message_history.content（平台消息历史）
        各自的 max_rowid 独立追踪，互不干扰。
        """
        with _FTS_LOCK:
            plugin_db = self._get_plugin_db_path()
            core_db = self._get_core_db_path()

            if not os.path.exists(core_db):
                logger.warning(f"Core database not found: {core_db}")
                return

            try:
                # 连接插件自有数据库（建表幂等）
                pconn = sqlite3.connect(plugin_db)
                pconn.execute("PRAGMA journal_mode=WAL;")
                pconn.execute("""
                    CREATE VIRTUAL TABLE IF NOT EXISTS conversations_fts
                    USING fts5(
                        conversation_id UNINDEXED,
                        content_text,
                        tokenize='unicode61'
                    );
                """)
                pconn.execute("""
                    CREATE VIRTUAL TABLE IF NOT EXISTS pmh_fts
                    USING fts5(
                        conversation_id UNINDEXED,
                        content_text,
                        tokenize='unicode61'
                    );
                """)
                pconn.execute("""
                    CREATE TABLE IF NOT EXISTS fts_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    );
                """)
                pconn.commit()

                # 迁移旧版 key 'max_rowid' → 'conv_max_rowid'
                cursor = pconn.execute(
                    "SELECT value FROM fts_meta WHERE key = 'max_rowid';"
                )
                old_row = cursor.fetchone()
                if old_row:
                    pconn.execute(
                        "INSERT OR REPLACE INTO fts_meta (key, value) VALUES ('conv_max_rowid', ?);",
                        (old_row[0],),
                    )
                    pconn.execute("DELETE FROM fts_meta WHERE key = 'max_rowid';")
                    pconn.commit()

                # 以只读模式连接 core DB（纳秒级开销）
                cconn = sqlite3.connect(f"file:{core_db}?mode=ro", uri=True)
                cconn.execute("PRAGMA busy_timeout=3000;")

                # ── conversations_fts 增量 ──
                cursor = pconn.execute(
                    "SELECT value FROM fts_meta WHERE key = 'conv_max_rowid';"
                )
                row = cursor.fetchone()
                last_conv_rowid = int(row[0]) if row else 0

                cursor = cconn.execute(
                    "SELECT MAX(inner_conversation_id) FROM conversations;"
                )
                max_conv_rowid = cursor.fetchone()[0] or 0

                if last_conv_rowid < max_conv_rowid:
                    logger.info(
                        f"Conv FTS delta: {last_conv_rowid} → {max_conv_rowid}"
                    )
                    self._build_conv_fts_delta(
                        pconn, cconn, last_conv_rowid, max_conv_rowid
                    )
                else:
                    logger.debug(
                        f"Conv FTS up-to-date: {last_conv_rowid}/{max_conv_rowid}"
                    )

                # ── pmh_fts 增量 ──
                cursor = pconn.execute(
                    "SELECT value FROM fts_meta WHERE key = 'pmh_max_rowid';"
                )
                row = cursor.fetchone()
                last_pmh_rowid = int(row[0]) if row else 0

                cursor = cconn.execute(
                    "SELECT MAX(id) FROM platform_message_history;"
                )
                max_pmh_rowid = cursor.fetchone()[0] or 0

                if last_pmh_rowid < max_pmh_rowid:
                    logger.info(
                        f"PMH FTS delta: {last_pmh_rowid} → {max_pmh_rowid}"
                    )
                    self._build_pmh_fts_delta(
                        pconn, cconn, last_pmh_rowid, max_pmh_rowid
                    )

                cconn.close()
                pconn.close()

            except Exception as e:
                logger.error(f"FTS5 index sync failed: {e}")
                # FTS 不可用时回退到原始暴力搜索

    def _build_conv_fts_delta(
        self,
        pconn: sqlite3.Connection,
        cconn: sqlite3.Connection,
        last_rowid: int,
        max_rowid: int,
    ):
        """增量构建 conversations_fts 索引（全量首次，delta 后续）。"""
        rows = cconn.execute(
            """
            SELECT inner_conversation_id, conversation_id, content
            FROM conversations
            WHERE content IS NOT NULL
              AND inner_conversation_id > ?
            ORDER BY inner_conversation_id;
            """,
            (last_rowid,),
        ).fetchall()

        insert_sql = (
            "INSERT INTO conversations_fts (rowid, conversation_id, content_text) "
            "VALUES (?, ?, ?);"
        )

        built = 0
        skipped = 0
        for inner_id, conv_id, content_json in rows:
            try:
                text = self._extract_plain_text(content_json)
                if text and text.strip():
                    pconn.execute(insert_sql, (inner_id, conv_id, text))
                    built += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.warning(f"Conv FTS skip {conv_id}: {e}")
                skipped += 1

        pconn.execute(
            "INSERT OR REPLACE INTO fts_meta (key, value) VALUES ('conv_max_rowid', ?);",
            (str(max_rowid),),
        )
        pconn.commit()
        logger.info(
            f"Conv FTS delta indexed: {built} conversations, {skipped} skipped "
            f"(rowid {last_rowid} → {max_rowid})"
        )

    def _build_pmh_fts_delta(
        self,
        pconn: sqlite3.Connection,
        cconn: sqlite3.Connection,
        last_rowid: int,
        max_rowid: int,
    ):
        """增量构建 pmh_fts 索引，同时将 (platform_id, user_id) 映射到 conversation_id。

        映射逻辑：按 (platform_id, user_id) 匹配 conversations 表中最近更新的对话。
        多个对话匹配同组时取最新一条，后续消息级匹配会做二次过滤。
        """
        rows = cconn.execute(
            """
            SELECT pmh.id, pmh.platform_id, pmh.user_id, pmh.content
            FROM platform_message_history pmh
            WHERE pmh.id > ?
            ORDER BY pmh.id
            """,
            (last_rowid,),
        ).fetchall()

        insert_sql = (
            "INSERT INTO pmh_fts (rowid, conversation_id, content_text) "
            "VALUES (?, ?, ?);"
        )

        built = 0
        skipped = 0
        for pmh_id, platform_id, user_id, content_json in rows:
            try:
                text = self._extract_pmh_text(content_json)
                if not text or not text.strip():
                    skipped += 1
                    continue

                # 映射到 conversation_id（取最新一条）
                conv_rows = cconn.execute(
                    """SELECT conversation_id FROM conversations
                       WHERE platform_id = ? AND user_id LIKE ?
                       ORDER BY updated_at DESC LIMIT 1""",
                    (platform_id, f"%{user_id}"),
                ).fetchall()
                conv_id = conv_rows[0][0] if conv_rows else ""

                pconn.execute(insert_sql, (pmh_id, conv_id, text))
                built += 1
            except Exception as e:
                logger.warning(f"PMH FTS skip id {pmh_id}: {e}")
                skipped += 1

        pconn.execute(
            "INSERT OR REPLACE INTO fts_meta (key, value) VALUES ('pmh_max_rowid', ?);",
            (str(max_rowid),),
        )
        pconn.commit()
        logger.info(
            f"PMH FTS delta indexed: {built} messages, {skipped} skipped "
            f"(id {last_rowid} → {max_rowid})"
        )

    @staticmethod
    def _extract_plain_text(content_json: str) -> str:
        """从对话历史的 JSON 中提取纯文本，用于 FTS 索引。"""
        try:
            history = json.loads(content_json) if content_json else []
        except (json.JSONDecodeError, TypeError):
            return ""

        texts = []
        for msg in history:
            if not isinstance(msg, dict):
                continue
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            if isinstance(content, str) and content:
                texts.append(f"[{role}] {content}")
            elif isinstance(content, list):
                parts = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        parts.append(item.get("text", ""))
                if parts:
                    texts.append(f"[{role}] {' '.join(parts)}")
        return "\n".join(texts)

    @staticmethod
    def _extract_pmh_text(content_json: str | dict) -> str:
        """从 platform_message_history 的 content JSON 中提取纯文本。

        pmh content 格式示例：
        {"type": "user", "message": [{"type": "plain", "text": "..."}]}
        {"type": "bot", "message": [{"type": "think", "think": "..."}]}
        """
        try:
            data = (
                json.loads(content_json)
                if isinstance(content_json, str)
                else content_json
            )
        except (json.JSONDecodeError, TypeError):
            return ""

        if not isinstance(data, dict):
            return ""

        messages = data.get("message", [])
        if not isinstance(messages, list):
            return ""

        texts = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            msg_type = msg.get("type", "")
            if msg_type == "plain":
                texts.append(msg.get("text", ""))
            elif msg_type == "think":
                texts.append(msg.get("think", ""))

        return " ".join(texts)

    # ──────────────── 混合 FTS5 搜索 ────────────────

    def _fts_search(self, query: str) -> set:
        """在两个 FTS5 表中混合搜索，去重合并后返回 conversation_id 集合。

        搜索策略：
        1. 短语匹配（双表）
        2. 短语无命中时，子 token 分词补全（双表）
        3. pmh_fts 的结果通过 conversation_id 字段自动去重
        """
        plugin_db = self._get_plugin_db_path()
        try:
            conn = sqlite3.connect(plugin_db)
            safe_query = self._fts_escape(query)

            hit_ids: set[str] = set()

            # ── 1. conversations_fts 短语搜索 ──
            try:
                rows = conn.execute(
                    "SELECT conversation_id FROM conversations_fts WHERE content_text MATCH ?;",
                    (safe_query,),
                ).fetchall()
                hit_ids.update(row[0] for row in rows)
            except Exception:
                pass

            # ── 2. pmh_fts 短语搜索 ──
            try:
                rows = conn.execute(
                    "SELECT conversation_id FROM pmh_fts WHERE content_text MATCH ?;",
                    (safe_query,),
                ).fetchall()
                hit_ids.update(row[0] for row in rows if row[0])
            except Exception:
                pass

            # ── 3. 子 token 补全（双表）──
            parts = [p.strip('"') for p in query.strip().split()]
            _spec_chars = '.,;:!@#$%^&*+=()[]{}<>?~`|\'"'
            _trans_tbl = str.maketrans(_spec_chars, " " * len(_spec_chars))

            for part in parts:
                sanitized = part.translate(_trans_tbl).strip()
                if not sanitized:
                    continue
                # conversations_fts
                try:
                    rows = conn.execute(
                        "SELECT conversation_id FROM conversations_fts WHERE content_text MATCH ?;",
                        (sanitized,),
                    ).fetchall()
                    hit_ids.update(row[0] for row in rows)
                except Exception:
                    pass
                # pmh_fts
                try:
                    rows = conn.execute(
                        "SELECT conversation_id FROM pmh_fts WHERE content_text MATCH ?;",
                        (sanitized,),
                    ).fetchall()
                    hit_ids.update(row[0] for row in rows if row[0])
                except Exception:
                    pass

            conn.close()
            return hit_ids
        except Exception as e:
            logger.warning(f"FTS search failed, falling back: {e}")
            return set()

    @staticmethod
    def _fts_escape(query: str) -> str:
        """FTS5 OR 模式——每个词用双引号包裹为短语查询，防止 tokenizer 拆散复合词（如 index.js）。"""
        parts = query.strip().split()
        if len(parts) > 1:
            return " OR ".join(f'"{p}"' for p in parts)
        return f'"{query}"'

    @staticmethod
    def _parse_time(time_str: str) -> int | None:
        """解析 ISO 时间字符串为 Unix 时间戳（秒）。"""
        if not time_str or not time_str.strip():
            return None
        time_str = time_str.strip()
        from datetime import datetime

        for fmt in [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ]:
            try:
                dt = datetime.strptime(time_str, fmt)
                return int(dt.timestamp())
            except ValueError:
                continue
        try:
            return int(time_str)
        except (ValueError, TypeError):
            return None

    # ──────────────── Tool ────────────────

    @filter.llm_tool(name="grepHistoryContext")
    async def grep_history_context(
        self,
        event: AstrMessageEvent,
        query: str,
        umo: list = [],
        role: list = ["user", "assistant"],
        start_time: str = "",
        end_time: str = "",
        max_results: int = 10,
        max_messages: int = 5,
    ) -> MessageEventResult:
        """搜索本 AstrBot 实例内所有对话上下文，根据关键词查找包含该文本的消息记录。

        Args:
            query(string): 要搜索的关键词或文本。
            umo(array): 可选，按多个会话 ID（unified_msg_origin）过滤。支持特殊值 "current" 自动替换为当前会话。不指定则搜索全部可访问的对话。
            role(array): 要检索的消息角色，可选 'user', 'assistant', 'tool'。默认只检索 user 和 assistant。
            start_time(string): 可选，起始时间，ISO 格式如 '2026-06-01' 或 '2026-06-01 14:00:00'。
            end_time(string): 可选，结束时间，格式同上。
            max_results(number): 最大返回结果数，默认 10，最大 50。
            max_messages(number): 每个对话最大显示的消息条数，默认 5，最大 20。
        """
        max_results = max(1, min(max_results, 50))
        max_messages = max(1, min(max_messages, 20))
        query = query.strip()
        if not query:
            return "搜索关键词不能为空。"

        # 角色过滤：默认 user + assistant
        valid_roles = {"user", "assistant", "tool"}
        if not role:
            target_roles = {"user", "assistant"}
        else:
            target_roles = set(r.lower() for r in role if r.lower() in valid_roles)
            if not target_roles:
                target_roles = {"user", "assistant"}

        # 时间过滤解析
        ts_start = self._parse_time(start_time) if start_time else None
        ts_end = self._parse_time(end_time) if end_time else None
        time_filter_active = ts_start is not None or ts_end is not None

        # ── 权限检查 ──
        sender_id = event.get_sender_id()
        admins = self.context._config.get("admins_id", [])
        is_admin = sender_id in admins

        if self.admin_only and not is_admin:
            if umo:
                for uid in umo:
                    if uid != sender_id and uid != "current":
                        return (
                            "⚠️ 权限不足：当前设置为仅管理员可查询其他用户的对话内容。\n"
                            f"你只能搜索自己的对话（{sender_id}），umo 中包含非本人会话。"
                        )

        conv_mgr = self.context.conversation_manager
        if umo:
            effective_umos = [
                event.unified_msg_origin if uid == "current" else uid for uid in umo
            ]
        elif self.admin_only and not is_admin:
            effective_umos = [sender_id]
        else:
            effective_umos = None

        # ── 初始化 FTS5 索引（线程池执行，不阻塞事件循环） ──
        import asyncio

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._ensure_fts_index)

        # ── 双表混合 FTS5 搜索 ──
        fts_hit_ids: set[str] | None = None
        loaded_via_fts = False

        fts_hit_ids = await loop.run_in_executor(None, self._fts_search, query)
        if fts_hit_ids is not None and len(fts_hit_ids) == 0:
            return f"未找到包含「{query}」的对话记录。"
        loaded_via_fts = True

        # ── 加载对话（FTS 预过滤 + 去重） ──
        if loaded_via_fts and fts_hit_ids:
            if effective_umos:
                all_convs = []
                for uid in effective_umos:
                    batch = await conv_mgr.get_conversations(unified_msg_origin=uid)
                    all_convs.extend(batch)
            else:
                all_convs = []
                page = 1
                while True:
                    batch, total = await conv_mgr.get_filtered_conversations(
                        page=page, page_size=100
                    )
                    all_convs.extend(batch)
                    if page * 100 >= total:
                        break
                    page += 1

            convs = []
            seen = set()
            for conv in all_convs:
                if conv.cid in seen:
                    continue
                seen.add(conv.cid)
                if conv.cid in fts_hit_ids:
                    convs.append(conv)
                    if len(convs) >= max_results:
                        break
        else:
            # 回退：全量加载 + Python 暴力匹配
            if effective_umos:
                convs = []
                for uid in effective_umos:
                    batch = await conv_mgr.get_conversations(unified_msg_origin=uid)
                    convs.extend(batch)
            else:
                convs = []
                page = 1
                while True:
                    batch, total = await conv_mgr.get_filtered_conversations(
                        page=page, page_size=100
                    )
                    convs.extend(batch)
                    if page * 100 >= total:
                        break
                    page += 1

        # ── 时间过滤（对话级） ──
        if time_filter_active:
            filtered = []
            for conv in convs:
                ct = getattr(conv, "created_at", None)
                if ct is None or not isinstance(ct, (int, float)):
                    filtered.append(conv)
                    continue
                if ts_start is not None and ct < ts_start:
                    continue
                if ts_end is not None and ct > ts_end:
                    continue
                filtered.append(conv)
            convs = filtered
            if not convs:
                return f"未找到包含「{query}」的对话记录。"

        # ── 消息级匹配 ──
        results = []
        query_parts = [p.lower() for p in query.split()]

        for conv in convs:
            if len(results) >= max_results:
                break

            try:
                raw_history = (
                    conv["history"]
                    if isinstance(conv, dict)
                    else getattr(conv, "history", None)
                )
                history = json.loads(raw_history) if raw_history else []
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(
                    f"解析对话历史失败 ({conv['cid'] if isinstance(conv, dict) else conv.cid}): {e}"
                )
                continue

            if not history:
                continue

            matched_messages = []
            for msg in history:
                if len(matched_messages) >= max_messages:
                    break
                msg_role = (
                    msg.get("role", "unknown") if isinstance(msg, dict) else "unknown"
                )
                if msg_role not in target_roles:
                    continue
                content = self._extract_text(msg)
                if not content:
                    continue
                if any(p in content.lower() for p in query_parts):
                    display = content[:500] + ("..." if len(content) > 500 else "")
                    matched_messages.append({"role": msg_role, "content": display})

            if matched_messages:
                results.append(
                    {
                        "user": conv["user_id"]
                        if isinstance(conv, dict)
                        else conv.user_id,
                        "platform": conv["platform_id"]
                        if isinstance(conv, dict)
                        else conv.platform_id,
                        "conversation_id": conv["cid"]
                        if isinstance(conv, dict)
                        else conv.cid,
                        "title": conv["title"]
                        if isinstance(conv, dict)
                        else conv.title or "",
                        "matched_count": len(matched_messages),
                        "messages": matched_messages,
                    }
                )

        if not results:
            return f"未找到包含「{query}」的对话记录。"

        role_tag = ",".join(sorted(target_roles))
        time_tag = ""
        if ts_start is not None:
            from datetime import datetime as _dt

            time_tag += f" 起: {_dt.fromtimestamp(ts_start).strftime('%Y-%m-%d %H:%M')}"
        if ts_end is not None:
            from datetime import datetime as _dt

            time_tag += f" 止: {_dt.fromtimestamp(ts_end).strftime('%Y-%m-%d %H:%M')}"
        lines = [f"## 搜索结果：{query}\n> 检索角色: {role_tag}{time_tag}\n"]
        for i, r in enumerate(results, 1):
            lines.append(
                f"### {i}. 用户: `{r['user']}` | 平台: {r['platform']}"
            )
            if r["title"]:
                lines.append(f"   标题: {r['title']}")
            lines.append(f"   对话ID: `{r['conversation_id']}`")
            lines.append(f"   匹配消息数: {r['matched_count']}")
            for j, m in enumerate(r["messages"], 1):
                lines.append(f"   [{j}] ({m['role']}) {m['content']}")
            lines.append("")

        return "\n".join(lines)

    # ──────────────── 辅助方法 ────────────────

    @staticmethod
    def _extract_text(msg: dict) -> str:
        content = msg.get("content", "") if isinstance(msg, dict) else ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(item.get("text", ""))
            return " ".join(parts)
        return ""

    async def terminate(self):
        logger.info("GrepHistoryContextPlugin v3 terminated.")
