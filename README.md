# 特征平台派生集服务

纯 Python 标准库 + SQLite 实现的后端服务。JSON 模型描述一张源表和一张
有向无环节点图（投影 / 去重 / 等值左连接 / 分组计数），服务提供建模、
批次提交、节点查询、版本零出口快照、以及按版本续取的出口变更接口。

- 关系求值完全由 `feature_service/engine.py` 自行实现（有序、带重数的
  行袋），**SQLite 只做持久化**，不参与任何连接/聚合计算。
- 源码：`feature_service/`；测试与展开重复行对拍：`tests/`。

## 目录

```
feature_service/
  engine.py   模型校验、关系求值、行序、净差计算、批次载荷校验
  store.py    SQLite 持久化（模型、源行、各节点结果、版本、净差、批次号）
  api.py      JSON HTTP 接口（http.server，无第三方依赖）
  cli.py      启动入口
tests/
  test_model.py         模型校验（标识、类型、引用、环、形状）
  test_engine.py        四类算子语义、排序、净差
  test_api.py           HTTP 接口、幂等、拒绝不改状态、并发
  test_continuation.py  按版本续取、窗口重放、节点快照一致
  test_differential.py  与“展开重复行”朴素实现对拍 + 重启全量重放
  reference.py          朴素参考实现（逐条展开行、嵌套循环）
  helpers.py            HTTP 客户端与可重启的测试服务
docs/DESIGN.md          设计说明与对拍矩阵
```

## 运行

```bash
python3 -m feature_service.cli --db /tmp/feature.db --port 8080
# 或
python3 -m http.server  # 不需要；服务自带 HTTP
```

测试（仅标准库 unittest）：

```bash
python3 -m unittest discover tests -v
```

## 数据模型

```json
{
  "version": 0,
  "source": {
    "name": "src",
    "columns": [{"type": "int", "nullable": false},
                {"type": "int", "nullable": true},
                {"type": "str", "nullable": true}]
  },
  "nodes": {
    "proj":   {"type": "projection", "inputs": ["src"], "columns": [1, 2]},
    "dedup":  {"type": "dedup", "inputs": ["proj"]},
    "joined": {"type": "left_join", "inputs": ["src", "dedup"],
               "columns": {"left": [1], "right": [0]}},
    "counts": {"type": "group_count", "inputs": ["joined"],
               "keys": [1], "count": 3},
    "global": {"type": "group_count", "inputs": ["src"], "keys": [], "count": 2}
  },
  "exits": ["dedup", "joined", "counts", "global"]
}
```

规则摘要：

- **标识**：源表名、节点名、出口名都是唯一非空 ASCII 串（可打印、无空格，
  并排除 `/ % ? #` 以便直接用于 URL 路径）；节点名不得与源表同名。
- **列**：`{"type": "int"|"str", "nullable": bool}`；也接受 `"int"`/`"str"`
  简写（视作可空）。列只含可空整数或字符串，初始源表为空。所有列引用都是
  **零基位置**。建模型时校验类型、引用范围与无环。
- **版本**：建模成功记版本 0，并持久化“空源求值”的初始出口快照；不发布
  `null`/变更。`POST /model` 对同一结构幂等，返回原版本。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/model` | 建模；400 校验失败、409 结构不同的模型、幂等返回 0 |
| GET | `/model` | 当前版本、源、节点、出口摘要 |
| GET | `/source` | 源行（`rows` 逐条展开、`row_weights` 聚合重数） |
| GET | `/nodes/<name>` | 当前版本下该节点持久化结果 |
| GET | `/nodes/<name>?evaluate=1` | 用当前源行现场重算该节点（对拍用） |
| GET | `/exits`、`/exits/<name>` | 当前发布的出口行 |
| GET | `/snapshot` | 版本 0 出口快照（持久化，不随版本变化） |
| POST | `/batches` | 提交批次 |
| GET | `/changes?cursor=N[&since=M]` | 取版本严格大于 N 的整批变更 |

提交载荷：

```json
{"batch_id": "b-001", "source": "src",
 "changes": [{"row": [1, 7, "x"], "diff": 2},
             {"row": [2, null, null], "diff": 1}]}
```

- `batch_id` 唯一；`source` 必须是模型源表名；`diff` 为非零整数，**整数
  禁止布尔**（`true`/`false`、浮点同样拒绝）。
- 同批同源同值先合并重数，合并后的最终源行重数不得为负，整批要么全部
  生效要么全部拒绝；拒绝不改状态、不改版本。
- 成功版本加一，发布各出口相对提交前的**净重数差**：按行合并、零差省略。
- 同 ID、同结构（同批同源同值合并后的等价内容，与条目顺序无关）返回原
  版本；异内容返回 409。并发提交等价于某个串行顺序。

行序：按列排序，整数按数值、字符串按 UTF-8 字节序、`null` 置后；相等行
连续输出，重数即重复次数。接口中 `rows` 直接给出展开后的行数组（每条重复
一行），`row_weights` 给出 `{row, weight}` 形式，便于逐条读取或按重数压缩。

## 语义要点

- **投影**：按指定列序（可重复列）保留全部重复行。
- **去重**：整行去重，`null` 与 `null` 相等；正重数一律变 1。
- **等值左连接**：键为两侧同类型、等长非空列数组；任一键分量为 `null`
  均不匹配；输出左右列拼接，匹配时重数相乘；无匹配则右侧补 `null`，
  左行重数不变。
- **分组计数**：指定键列数组与计数列；输出键、总行数（按重数累计）、计数
  列非空行数（按重数累计）。键为空表示全局组；空输入仍输出两个零。其他
  空输入（非空键）不输出。分组与去重中 `null` 相等。

## 一致性、原子性与重启

- 每次提交在一个 `BEGIN IMMEDIATE` 事务中把**源行、全部节点结果、版本
  号、出口净差、批次号**一起写入；任何步骤失败整体回滚，版本不变。
- 读接口在单一读事务/锁内读取版本与行，查询只会看到完整版本。
- `exit_initial` 表保存版本 0 快照，重启后 `GET /changes` 从版本 0 起
  重放全部净差即可还原当前出口 —— 见 `tests/test_differential.py` 与
  `tests/test_continuation.py` 的重启用例。
- 说明与对拍矩阵见 `docs/DESIGN.md`。
