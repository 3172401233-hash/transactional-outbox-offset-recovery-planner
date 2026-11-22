# transactional-outbox-offset-recovery-planner
恢复事务 outbox 与消息偏移之间的原子发布

## 场景

业务事务已提交、outbox 记录已落库，但 broker 确认（ack）丢失。消费者可能
已经处理了消息，也可能从未收到，offset 处于模糊状态。本规划器对每条消息
输出**确定性动作**与**可证明的消费状态**，并显式列出不可判定项。

## 幂等键

- `message_id` 即幂等键，由业务事务派生（如 `tx-1:order-7`），在首次发布
 与所有补发之间保持不变。
- 消费者侧以幂等键去重，但只在**去重窗口**（`RecoveryContext.dedup_window`，
 以 offset 距离计）内保证保留键。窗口内补发是安全的；窗口过期后补发可能
 产生真实重复，必须转入人工处理。
- 绝不能仅凭"broker 无记录"就重新发布——ack 丢失不代表消息未到达。

## 提交边界

1. **业务事务边界**：业务写入与 outbox 行在同一数据库事务内提交。outbox
 行存在而事务未提交属于数据损坏 → `MANUAL_REVIEW`。
2. **broker 确认边界**：`SENT` 只表示发送已尝试，不代表被接受。broker
 receipt（含 offset 与 producer epoch）才是消息已落日志的证据。receipt 的
 epoch 高于当前 producer epoch 时，本进程是僵尸生产者，被 fencing →
 `MANUAL_REVIEW`；旧 epoch 的 receipt（producer 重启场景）仍然有效 →
 `WAIT`，不重复发布。
3. **消费者 offset 边界**：仅当提交的 consumer offset 越过消息 offset、且
 重放日志中的提交序列单调（无 `OFFSET_ROLLBACK`、无倒退提交）时，才可证明
 已消费 → `SKIP`。回滚会使既有 offset 证明失效。绝不能仅凭 outbox 已发送
 就推进 offset。

## 决策规则（按序评估，输出确定）

| 条件 | 动作 | 消费状态 |
| --- | --- | --- |
| 业务事务未提交 | `MANUAL_REVIEW` | `UNKNOWN` |
| receipt epoch > 当前 producer epoch（fencing） | `MANUAL_REVIEW` | `UNKNOWN` |
| offset 证明已消费且日志单调 | `SKIP` | `PROVEN_CONSUMED` |
| 存在 broker receipt | `WAIT` | `UNKNOWN` |
| `PENDING`/`FAILED` 且无 receipt | `REPUBLISH` | `PROVEN_NOT_CONSUMED` |
| `SENT` 无 receipt，幂等键在去重窗口内 | `REPUBLISH` | `UNKNOWN` |
| `SENT` 无 receipt，窗口过期或不可判定 | `MANUAL_REVIEW` | `UNKNOWN` |

每个 `Decision` 的 `undecidable` 字段列出无法判定的维度（如 `consumption`、
`dedup_window`）；`RecoveryPlan.undecidable` 汇总所有需要人工跟进的条目。

## 用法

```python
from outbox_recovery import (
    MessageFacts, OutboxStatus, RecoveryContext, plan_recovery,
)

plan = plan_recovery(
    [MessageFacts(message_id="tx-1:order-7", tx_committed=True,
                  outbox_status=OutboxStatus.SENT, producer_epoch=4,
                  expected_offset=40, consumer_offset=38)],
    RecoveryContext(dedup_window=10),
)
for decision in plan.decisions:
    print(decision.message_id, decision.action.value, decision.reason)
```

## 测试

```sh
python -m unittest
```

覆盖确认丢失、重复投递、消费者回滚、producer 重启（epoch fencing）与去重
窗口过期，以及决策的确定性。代码兼容 Python 3.8+。
