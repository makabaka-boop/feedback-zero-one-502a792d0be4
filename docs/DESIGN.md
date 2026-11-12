# 设计说明与对拍矩阵

## 1. 目标与边界

服务把“特征平台组合采集表形成的派生集”建模为一张源表 + 一张有向无环节点
图。重复样本的补录（正 diff）与撤回（负 diff）会改变源行重数，进而反向改变
外连接补空行（`null` 补齐）和去重、分组结果。服务必须：

1. 校验模型（类型、列引用、无环、标识）；
2. 版本 0：空源求值，持久化出口快照，不发布变更；
3. 批次提交：合并同值重数、校验非负、整批原子生效；
4. 发布各出口相对提交前的净重数差（同值合并、零差省略）；
5. 支持节点查询与按版本续取出口变更；
6. 关系求值自实现，SQLite 仅持久化；
7. 并发等价串行，查询只见完整版本；重启后从版本 0 快照重放全部净差还原
   当前出口。

## 2. 关系表示与求值

`engine.py` 中的 `Table` 是 `(columns, rows)`，`rows: dict[tuple, int]`，
`int > 0` 为重数。所有算子是“行袋 → 行袋”的纯函数：

| 算子 | 规则 |
| --- | --- |
| projection | 按给定零基列序（允许重复列）取值，重数透传（保留重复） |
| dedup | 整行去重，`null == null`，正重数一律变 1 |
| left_join | 键数组等长非空同类型；任一侧键含 `null` 不匹配；匹配重数相乘；无匹配右补 `null`、左重数不变；输出左列拼接右列 |
| group_count | 键数组（空 = 全局组）分组，输出键 + 总重数 + 计数列非空重数；空输入且键为空 → `(0,0)`；空输入且键非空 → 无输出；`null == null` |

行序：`tuple_key` 逐列排序，整数按数值、字符串按 UTF-8 字节、`null` 最后；
输出时相等行连续、按重数重复（`rows` 为展开数组，`row_weights` 为压缩形式）。

### 为什么净差可加

每个算子都是确定性的行袋函数，只做加、乘、合并相等行与比较，不依赖行的
排列或历史。因此对任意出口行 `r`：

```
delta_version(r) = weight(source_after)(r) - weight(source_before)(r)
```

把该净差写入变更表、并对源行重数应用同一批 `diff`，出口行重数正好按同一
函数更新。于是：

```
exit(version N) = exit_snapshot(version 0) + Σ_{v=1..N} published_diffs(v)
```

这个等式由 `tests/test_differential.py::ReplayTests` 与
`tests/test_continuation.py::SnapshotWindowTests` 在重启后逐版本验证。提交
时 `api.py` 还会对每个节点与每个出口做一次 `apply_deltas(old, deltas) ==
new` 自检，不成立即 500 拒绝（不会写库）。

## 3. 持久化与原子性

`store.py` 的表：

```
model(id=1, model_json, model_sig, source_name, source_columns, version)
node_columns(node_name, columns)        -- 每个节点的输出列
node_rows(node_name, row_json, weight)  -- 每个节点在当前版本的结果（含出口）
source_rows(row_json, weight)           -- 源行重数
exit_initial(exit_name, row_json, weight) -- 版本 0 出口快照，不可变
versions(version)                       -- 已提交版本（含零净差批次）
version_diffs(version, exit_name, row_json, diff)
batches(batch_id, canonical, version)
```

一次提交在单个 `BEGIN IMMEDIATE` 事务中依次写：节点差、出口净差、版本号、
源行、批次号、版本记录，任一步失败整体回滚。`commit_batch` 在事务内重新读取
当前版本并与计算时观察到的版本比对；不等则回滚并让上层重算（最多 8 次），
最终仍冲突返回 409。读接口用只读事务/互斥锁一次取出版本与行，查询永远看到
完整版本。

- 拒绝路径（模型非法、批次非法、负重数、结构不同的模型）在任何写之前抛错，
  因此状态与版本都不变。
- 同 `batch_id` 重复：比较 `canonical` 签名（同批同源同值合并后，与条目顺序
  无关）。相同返回原版本且 `replayed=true`；不同则 409。
- 并发：单进程多线程 + 多进程（同库两个 server）测试均通过，版本严格递增、
  每批恰好加一。

## 4. 接口

见 `README.md`。要点：

- `POST /model`：首次注册保存模型与版本 0 各节点结果；返回 `nodes` 与
  `exits`（同一求值结果）。同结构重复注册返回 `replayed=true`、版本不变。
- `POST /batches`：返回 `version`、`replayed`、`changes`（出口净差）、
  `node_changes`（全部节点净差，便于对拍）。
- `GET /changes?cursor=N`：只返回版本 `> N` 的整批，`batches` 按版本分组，
  `changes` 为扁平列表；`since=M` 限定到某一版本；省略 `cursor` 表示从版本 0
  快照续取。
- `GET /snapshot`：持久化的版本 0 出口快照。

## 5. 展开重复行对拍矩阵

对拍方法：把源行按重数展开成逐条行列表，交给 `tests/reference.py` 的朴素
实现（逐条列表、嵌套循环），与正式引擎的规范展开结果逐行比较。随机模型
（25 组、0–8 行、含 `null`、重复字符串、不同重数）全部一致。

| # | 场景 | 断言 | 用例 |
| --- | --- | --- | --- |
| 1 | 补空行撤回恢复 | 撤回唯一右匹配后，外连接由匹配行恢复为 `(5, None)` 补空行；净差同时含删除匹配行与新增补空行 | `test_differential.py::test_withdraw_supplier_restores_null_padded_row` |
| 2 | 去重跨零 | 源重数 2 → 1 时去重结果不变、发布零差；重数 → 0 才删除 | `test_engine.py::DedupTests`、`test_continuation.py::test_dedup_crossing_zero_publishes_no_change` |
| 3 | 空全局组 | 空输入、空键的输出 `(0,0)`；空输入、非空键无输出 | `test_engine.py::GroupCountTests` |
| 4 | 非 null 匹配键自连接 1→2 | 左重数从 1 加到 2，右表同步变 2，结果 `(7,7)` 重数 1 → 4；净差 `+3` | `test_engine.py::test_self_join_one_to_two_doubles_then_doubles_again` |
| 5 | 重启全量重放 | 重启后 `snapshot + changes` 累积应用 == 当前出口；每版本窗口同样成立 | `test_differential.py::ReplayTests`、`test_continuation.py` |
| 6 | 节点结果一致性 | 每个节点“持久化结果 == 现场重算结果” | `test_continuation.py::test_persisted_nodes_match_fresh_evaluation_every_version` |
| 7 | 拒绝不改状态 | 负重数、非法 bool/float/null、异内容 batch_id 均返回错误且版本/源不变 | `test_api.py::BatchTests` |
| 8 | 并发 | 多线程 8 批与双 server 6 批版本恰为 1..N；同批并发只生效一次 | `test_api.py::ConcurrencyTests` |

## 6. 已知取舍

- 每个节点都持久化结果（而非仅出口），使节点查询只读且与出口同版本；代价
  是每次提交写全部节点净差。这满足“节点结果、版本和变更原子持久化”。
- 分组计数的“总行数”按键重数累计；去重/分组视为 `null == null`。
- `row_json` 使用 JSON 文本作为值键：整数与字符串天然区分（`1` vs `"1"`），
  `null` 为 `null`。
- 行数量级受 `MAX_WEIGHT` 保护（10^100），超过即拒绝该批次。
- 标识符额外排除 `/ % ? #` 与空格，避免 URL 路径歧义；这比“可打印 ASCII”
  更严格但保持了“非空 ASCII 串”的要求。

## 7. 运行与验证

```bash
python3 -m unittest discover tests -v     # 98 个用例
python3 -m feature_service.cli --db /tmp/feature.db --port 0
```

无第三方依赖，仅 Python 3.10+ 标准库（使用了 `match` 的赋值表达式、
`http.server`、`sqlite3`）。
