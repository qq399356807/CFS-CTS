# CFS-CTS Core 0.1.3-dev — 实测报告

验证运行：[GitHub Actions #45376452928](https://github.com/qq399356807/CFS-CTS/actions/runs/45376452928)

环境：GitHub Actions，Python 3.11–3.13 × Ubuntu / Windows / macOS（9 个矩阵任务）。

汇总：**26 PASS / 0 FAIL / 0 BLOCKED / 0 INCONCLUSIVE**

| ID | 测试 | 状态 |
|---|---|---|
| CORE-01 | UTC rollback | **PASS** |
| CORE-02 | UTC jump | **PASS** |
| CORE-03 | event deletion | **PASS** |
| CORE-04 | content tamper | **PASS** |
| CORE-05 | event reorder | **PASS** |
| CORE-06 | local ledger+anchor rebuild boundary | **PASS** |
| CORE-07 | simulated reboot | **PASS** |
| SEC-01 | concurrent writes | **PASS** |
| SEC-02 | duplicate event ID | **PASS** |
| SEC-03 | partial write/crash tail | **PASS** |
| SEC-04 | JSONL truncation | **PASS** |
| SEC-05 | anchor rollback boundary | **PASS** |
| SEC-06 | missing parent | **PASS** |
| SEC-07 | oversized event | **PASS** |
| SEC-08 | Unicode/canonicalization | **PASS** |
| SEC-09 | exact duplicate batch retry is idempotent | **PASS** |
| SEC-10 | invalid clock input | **PASS** |
| SEC-11 | single-event lock failure is fail-closed | **PASS** |
| SEC-12 | intermediate anchor checkpoint tamper | **PASS** |
| SEC-13 | conflicting batch retry rejected | **PASS** |
| SEC-14 | incomplete batch retry rejected | **PASS** |
| SEC-15 | batch causal ordering enforced | **PASS** |
| SEC-16 | batch lock failure is fail-closed | **PASS** |
| SEC-17 | historical anchor prefix mismatch rejected | **PASS** |
| SEC-18 | anchor checkpoint sequence range enforced | **PASS** |
| SEC-19 | valid historical anchor prefixes accepted | **PASS** |

> CORE-06 的 PASS 仅表示本地同时重建账本与模拟锚点的边界演示，不代表抵御同权限攻击者。当前没有配置外部锚点。

## 风险与未实现

- 同权限攻击者可同时改写本地账本与模拟锚点；本次改动只验证二者内部历史一致性。
- 无独立管理的远程锚点。
- 批量追加具备重试幂等性，但不是断电/文件系统故障下的原子事务；不完整批次需要取证审查。
- 文件锁和 fsync 的保证依赖操作系统及文件系统。
- SHA-256 未使用密钥，不能抵御可改写账本的攻击者。
