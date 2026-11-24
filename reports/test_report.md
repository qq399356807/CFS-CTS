# CFS-CTS Core 0.1.2 — 实测报告

验证提交：`34d7190c81058dcf21ade0cd04ea6d629998259a`

验证来源：[GitHub Actions run #45371260968](https://github.com/qq399356807/CFS-CTS/actions/runs/45371260968)

运行矩阵：Python 3.11、3.12、3.13 × Ubuntu、Windows、macOS，共 9 个 CI jobs。

汇总：**23 PASS / 0 FAIL / 0 BLOCKED / 0 INCONCLUSIVE**

| ID | 测试 | 状态 |
|---|---|---|
| CORE-01 | UTC rollback | PASS |
| CORE-02 | UTC jump | PASS |
| CORE-03 | event deletion | PASS |
| CORE-04 | content tamper | PASS |
| CORE-05 | event reorder | PASS |
| CORE-06 | local ledger+anchor rebuild boundary | PASS |
| CORE-07 | simulated reboot | PASS |
| SEC-01 | concurrent writes | PASS |
| SEC-02 | duplicate event ID | PASS |
| SEC-03 | partial write/crash tail detection | PASS |
| SEC-04 | JSONL truncation | PASS |
| SEC-05 | anchor rollback boundary | PASS |
| SEC-06 | missing parent rejection | PASS |
| SEC-07 | oversized event rejection | PASS |
| SEC-08 | Unicode/canonicalization | PASS |
| SEC-09 | exact duplicate batch retry is idempotent | PASS |
| SEC-10 | invalid clock input | PASS |
| SEC-11 | single-event lock failure is fail-closed | PASS |
| SEC-12 | intermediate anchor checkpoint tamper | PASS |
| SEC-13 | conflicting batch retry rejected | PASS |
| SEC-14 | incomplete batch retry rejected | PASS |
| SEC-15 | batch causal ordering enforced | PASS |
| SEC-16 | batch lock failure is fail-closed | PASS |

> CORE-06 的 PASS 仅表示本地同时重建账本与模拟锚点的限制得到复现，不代表系统能抵御同权限攻击者。外部锚点仍未配置。

## 风险与未实现

- 批量提交具有应用层重试幂等性，不构成断电或文件系统故障下的原子事务。
- 不完整批次会被检测并拒绝自动修复；需保留现场并进行人工取证审查。
- 尚未配置独立管理的远程锚点。
- 文件锁和 fsync 的保证仍取决于操作系统、文件系统和实际运行环境。
- SHA-256 未加密钥；能改写账本的攻击者可以重新计算哈希链。

本报告记录上述 GitHub Actions 验证提交的矩阵结果。每个平台的详细测试报告保存在对应 CI artifact 中。
