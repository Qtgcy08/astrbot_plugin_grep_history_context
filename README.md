# Grep History Context v3

> 跨会话对话内容搜索引擎——LLM 在 AstrBot 实例内按关键词搜索历史对话，FTS5 双索引 + 近实时增量同步。

## 架构

```
                      ┌──────────────────┐
                      │  LLM / Tool 调用  │
                      │  grepHistoryContext │
                      └────────┬─────────┘
                               │
                    ┌──────────▼──────────┐
                    │  _ensure_fts_index   │   ← 每次搜索前检查增量
                    │  线程安全 · 幂等     │
                    └──────────┬──────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
  ┌───────────────┐   ┌───────────────┐   ┌────────────────┐
  │ conversations_fts│  │   pmh_fts     │   │  fts_meta      │
  │ 对话上下文       │   │ 平台消息历史   │   │ conv_max_rowid │
  │ FTS5 索引       │   │ FTS5 索引     │   │ pmh_max_rowid  │
  └───────┬───────┘   └───────┬───────┘   └────────────────┘
          │                   │
          └───────┬───────────┘
                  ▼
          ┌───────────────┐
          │ set 合并去重   │
          │ conversation_id │
          └───────┬───────┘
                  ▼
          ┌───────────────┐
          │ 加载对话全文   │
          │ 消息级二次匹配  │
          │ 格式化输出     │
          └───────────────┘
```

## 为什么需要双索引？

| 场景 | conversations_fts | pmh_fts |
|:---|:---:|:---:|
| 完整多轮对话历史 | ✅ 命中 | ✅ 命中 |
| 被 GC 截断/压缩的对话 | ❌ 丢失 | ✅ 兜底 |
| 单条平台消息（思考链） | ❌ 无法覆盖 | ✅ 可追溯 |
| 按 conversation_id 合并 | 原生自带 | (platform_id, user_id) 映射 |

**核心设计取舍：** conversations 表存的是 LLM 对话上下文，某些旧对话会被压缩截断，导致关键词丢失。pmh 表存的是原始平台消息（platform_message_history），每条消息独立保存——丢失的关键词在这里被找回。两个 FTS 表同时搜索，结果按 conversation_id 去重合并。

## V3 改进

- **双 FTS5 索引**：`conversations_fts` + `pmh_fts`，覆盖完整 + 截断对话
- **增量同步**：每次搜索前检查 `fts_meta` 水位，只索引新增行
- **混合搜索**：短语精准匹配 + 子 token 补全，双表并行
- **自动去重**：`pmh_fts` 通过 `(platform_id, user_id)` 映射回 `conversation_id`，set 合并
- **线程安全**：`threading.Lock` 保护增量构建，幂等重复调用
- **零阻塞**：FTS 索引构建和搜索均在 `run_in_executor` 中执行，不阻塞事件循环

## 安装

```bash
# 方案一：从插件市场安装
# 搜索 "Grep History Context"

# 方案二：从源码安装
cd /path/to/astrbot/core/data/plugins/
git clone https://github.com/Qtgcy08/astrbot_plugin_grep_history_context.git
# 或 zip 解压至 plugins 目录后执行
/dev_automation reload
```

## 配置

`_conf_schema.json`：

```json
{
  "admin_only": {
    "type": "bool",
    "description": "是否仅允许管理员查询其他用户的对话",
    "default": true,
    "required": true
  }
}
```

`admin_only = true`（默认）：非管理员只能搜索自己的对话。
`admin_only = false`：任何用户可跨会话搜索。

## 使用

插件注册了一个 LLM Tool，LLM Agent 在思考过程中自动调用。

### Tool 参数

| 参数 | 类型 | 默认 | 说明 |
|:---|:---|:---:|:---|
| `query` | string | 必填 | 要搜索的关键词或文本 |
| `umo` | array | 全部 | 按会话 ID (unified_msg_origin) 过滤，支持 `"current"` |
| `role` | array | `["user","assistant"]` | 检索的消息角色：`user` / `assistant` / `tool` |
| `start_time` | string | 无 | ISO 起始时间，如 `"2026-06-01"` |
| `end_time` | string | 无 | ISO 结束时间 |
| `max_results` | number | 10 | 最大返回对话数（1~50） |
| `max_messages` | number | 5 | 每个对话最大显示消息数（1~20） |

### 搜索结果格式

```
## 搜索结果：关键词
> 检索角色: user,assistant

### 1. 用户: `webchat:FriendMessage:...` | 平台: webchat
   标题: 某次对话标题
   对话ID: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
   匹配消息数: 3
   [1] (user) 消息内容...
   [2] (assistant) 回复内容...
```

## 数据存储位置

```
core/data/plugin_data/astrbot_plugin_grep_history_context/
└── fts_index.db          ← 自动构建的 FTS5 索引（SQLite）
```

索引数据库自动创建，首次搜索时增量构建，后续搜索前检查水位线，仅索引新增数据。

## 性能

- **首次构建**：~1 秒（本地测试，115 条对话 + 6147 条平台消息，约 400MB data_v4.db）
- **增量搜索**：< 10ms（FTS5 短语匹配）
- **内存**：FTS 索引约 42MB（首次全量）

## 版本

当前版本：**v3.0.0**

- v3（2026-07-26）：双 FTS5 索引 + 增量同步 + pmh 兜底 + 线程安全
- v2：单 FTS5 索引（仅 conversations）
- v1：暴力遍历全量对话

## 作者

闻人墨

## 仓库

https://github.com/Qtgcy08/astrbot_plugin_grep_history_context
