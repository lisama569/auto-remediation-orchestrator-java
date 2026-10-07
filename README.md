# 本地故障注入与自动修复编排服务

基于 Spring Boot 的进程内故障注入与自动修复编排服务。被编排的实例由进程内
`Target` 状态对象模拟，不依赖任何外部环境，保证并发触发与修复失败时的判定
一致、可复现且可解释。

## 构建与运行

```bash
mvn compile        # 编译
mvn test           # 运行全部测试
mvn spring-boot:run  # 启动服务（默认 8080 端口）
```

## 状态机

### 计划状态机（FaultInjectionPlan）

```
                 submit
                   │
        ┌──────────┴──────────────────────────┐
        ▼                                       ▼
    REJECTED               PENDING ──▶ RUNNING ──┬─▶ SUCCEEDED   (修复成功，健康校验通过)
   （附带可区分的拒绝原因）                        ├─▶ ROLLED_BACK  (重试耗尽/超时/校验失败，已回滚)
                                                  └─▶ FAILED       (执行链路出现未预期异常)

   声明忙时排队(queueIfBusy=true)且目标忙：
       ACCEPTED(QUEUED) ──前序计划终态后出队──▶ 出队重判 ──通过──▶ RUNNING（同一目标同时仅一个）
                                              │
                                              └─失效──▶ REJECTED（附带可区分原因，稳定终态）
```

- `REJECTED` / `SUCCEEDED` / `ROLLED_BACK` / `FAILED` 均为终态，稳定且可区分。
- 新增的 `QUEUED` 为中间态：计划已被接受、在该目标的等待队列中，尚未开始执行。
  外部通过 `GET /api/plans/{id}` 可一眼区分"正在执行（RUNNING）"与"队列等待（QUEUED）"。
- 执行链路中任一步失败都会记录日志并落入对应终态，不会被静默吞掉。

## 忙时排队与容量控制

### 提交方式与兼容范围

- `submit(targetId, faultType, idempotencyKey)`（REST 不带 `queueIfBusy` 字段或为
  `false`）：**行为与历史完全一致**——目标忙时直接以 `PLAN_CONFLICT` 拒绝，
  原有四种拒绝原因、返回体与调用方处理逻辑均不变。
- `submit(..., queueIfBusy=true)`（REST 请求体 `"queueIfBusy": true`）：忙时排队。
  目标空闲时立即执行（PENDING→RUNNING）；目标忙时计划进入**该目标独立**的 FIFO
  等待队列，当前执行计划无论成功、回滚还是失败到达终态后，按提交先后自动接续；
  同一目标任何时刻最多只有一个计划真正执行。排队提交同样返回 HTTP 202，计划状态为
  `QUEUED`。

### 队列容量与 QUEUE_FULL

- 容量**按目标独立计数**，默认每目标 10，可配置：
  - 属性 `orchestrator.queue-capacity`（`application.properties`），可用环境变量
    `ORCHESTRATOR_QUEUE_CAPACITY` 覆盖；
  - 直接构造 `PlanOrchestrator` 时可传构造参数。
- 队列已满时，新的排队提交以新增拒绝原因 **`QUEUE_FULL`** 拒绝（HTTP 409），与
  `DUPLICATE_REQUEST` / `PLAN_CONFLICT` / `TARGET_QUARANTINED` /
  `OUTSIDE_MAINTENANCE_WINDOW` 明确区分，调用方可直接判定为"队列满了"。
- 被队满拒绝的请求**不占队列名额、不留任何残留状态**（计划不入 `plans` 存储、
  幂等键不登记，`GET /api/plans/{id}` 查不到该计划）；队列腾出位置后新的排队
  提交可正常进入。
- 容量判定与入队在"每目标一把锁"的临界区内完成；并发提交下容量**不会超卖**，
  已入队计划**不会丢失**，FIFO 顺序即线程在目标锁上的获取顺序。

### 出队重新判定与失效规则

排队计划在真正开始执行之前，会按目标**当下**状态重新判定，判定顺序与提交时一致：

1. 幂等键已不属于本计划 → `DUPLICATE_REQUEST`；
2. 目标已被隔离 → `TARGET_QUARANTINED`；
3. 目标维护窗口已关闭 → `OUTSIDE_MAINTENANCE_WINDOW`；
4. 全部通过 → 原子占位并异步接续执行。

- 失效计划**不会挂着不管，也不会被静默丢弃**：立即落入带明确原因的 `REJECTED`
  终态，随后继续判定队列中的下一个，直到取出可执行计划或队列腾空。
- 幂等键在计划被**接受**（立即执行或进入队列）的那一刻即原子登记，并贯穿
  "排队—执行—完成"全过程永久有效：排队中重复提交、执行中重复提交、终态后
  重放，都稳定返回同一个原计划并判定 `DUPLICATE_REQUEST`，绝不会重复执行，
  重复提交也不占队列名额。
- 注意：解除隔离（recover）只影响之后新提交的判定，不会主动唤醒仍在队列中的计划；
  它们统一在被调度出队时按当下状态收敛。


### 目标状态机（Target）

```
 HEALTHY ──注入故障──▶ FAULTY ──修复成功──▶ HEALTHY（consecutiveFailures 清零）
                         │
                         └─修复失败──▶ 回滚到执行前状态（HEALTHY，无残留故障）
                                       consecutiveFailures + 1
                                       达到阈值 ──▶ quarantined=true（隔离）
 隔离目标 ──recover──▶ 解除隔离、计数清零，重新参与调度
```

## 下发校验与拒绝原因

`PlanOrchestrator.submit` 按**固定顺序**校验，保证并发下拒绝原因稳定可区分：

| 顺序 | 条件 | 拒绝原因 |
|---|---|---|
| 1 | 幂等键已存在 | `DUPLICATE_REQUEST`（返回原计划，不重复执行） |
| 2 | 目标已隔离 | `TARGET_QUARANTINED` |
| 3 | 目标未处于维护窗口 | `OUTSIDE_MAINTENANCE_WINDOW` |
| 4a | 目标忙且**未**声明排队 | `PLAN_CONFLICT`（历史语义，保持不变） |
| 4b | 目标忙、声明排队但队列已满 | `QUEUE_FULL`（不占名额、不留残留） |

- 同一目标同一时刻只允许一个进行中的计划：针对同一目标的所有判定都在该目标的
  锁内串行完成，`activePlans` 原子占位只允许一个计划执行。
- 幂等键在计划被接受（立即执行或入队）时通过 `putIfAbsent` 原子登记；并发下同键
  竞争失败者按 `DUPLICATE_REQUEST` 处理，绝不造成重复执行。
- 并发下同键请求若先撞上隔离/窗口/忙等条件，会得到对应顺序的拒绝——各原因均为
  合法且可区分，关键不变量（同键只执行一次）始终成立。

## 修复策略语义（RemediationPolicy）

| 参数 | 含义 |
|---|---|
| `maxRetries` | 单次修复流程的最大重试次数（不含首次执行） |
| `backoffMillis` | 退避基数，第 n 次重试前退避 `n × backoffMillis`（线性退避） |
| `quarantineThreshold` | 连续失败达到该次数后目标进入隔离 |
| `actionTimeoutMillis` | 单次修复动作的超时时间；超时即中断该次动作 |

- **失败计数按"修复流程"计，不按单次尝试计**：一次流程内多次尝试失败只累加 1。
- 计数与隔离判定在 `TargetRegistry.update` 的 CAS 临界区内原子完成，
  并发下不会重复计数或丢失。
- 修复成功后 `consecutiveFailures` 清零。

## 隔离与回滚规则

- **回滚**：快照在注入故障之前采集。动作超时、动作异常或健康校验未通过且
  重试耗尽后，目标被恢复到执行前状态（健康状态与故障字段），不留下中间态；
  回滚只写本目标，不影响其他目标，也不会解除已有的隔离标志。
- **隔离**：连续失败达到阈值的目标被置为 `quarantined`，后续计划以
  `TARGET_QUARANTINED` 拒绝，不再重试、不再计数。
- **恢复**：`POST /api/targets/{id}/recover` 解除隔离并清零计数，
  目标重新参与后续调度。

## REST API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/targets` | 注册目标 `{targetId, inMaintenanceWindow}` |
| GET | `/api/targets/{id}` | 查询目标状态 |
| POST | `/api/targets/{id}/maintenance?open=` | 开关维护窗口 |
| POST | `/api/targets/{id}/recover` | 解除隔离 |
| POST | `/api/targets/{id}/behavior?behavior=` | 测试钩子：SUCCEED/FAIL/HANG/NOOP |
| POST | `/api/plans` | 下发计划 `{targetId, faultType, idempotencyKey, queueIfBusy?}`；接受 202（状态 PENDING 或 QUEUED），拒绝 409（含原因，含 `QUEUE_FULL`）。`queueIfBusy` 缺省为 false，老请求体无需改动 |
| GET | `/api/plans/{id}` | 查询计划状态（`RUNNING` 执行中 / `QUEUED` 排队等待 / 各终态） |

## 验证方法

```bash
mvn test
```

- `PlanOrchestratorConcurrencyTest`
  - 16 线程并发下发：恰好 1 个被接受，其余 `PLAN_CONFLICT`；
  - 重复下发（进行中与终态后）：均判定 `DUPLICATE_REQUEST` 且返回原计划；
  - 同键并发：恰好执行一次，重放仍返回原计划；
  - 隔离/维护窗口拒绝原因可区分。
- `PlanQueueTest`（新增，忙时排队与容量）
  - FIFO 接续执行：忙时入队、`QUEUED` 与 `RUNNING` 外部可区分，前序终态后按提交
    先后自动接续，同一目标同时仅一个在执行；
  - 容量边界：队列达上限后新排队提交判 `QUEUE_FULL`，不占名额、查无残留；腾出
    名额后新提交可正常入队；8 线程并发提交（容量 2）恰好入队 2 个、其余全部
    `QUEUE_FULL`，且入队计划全部执行成功，无超卖、无丢失、无重复；
  - 排队期间状态收敛：等待中目标被隔离 / 维护窗口被关闭，出队重判后落入带
    `TARGET_QUARANTINED` / `OUTSIDE_MAINTENANCE_WINDOW` 原因的 `REJECTED`
    终态；恢复 / 窗口重开后新计划可正常执行；
  - 排队阶段同幂等键重复提交（排队/不排队两种调用方式）：始终判
    `DUPLICATE_REQUEST` 并返回同一个排队计划，全程只执行一次；
  - 不声明排队的忙时提交仍判 `PLAN_CONFLICT`，历史语义不变。
- `RemediationEngineTest`
  - 动作超时（HANG）→ `ROLLED_BACK`，目标恢复执行前状态、无中间态；
  - 健康校验未通过（NOOP）→ `ROLLED_BACK`；
  - 连续失败达阈值 → 隔离、停止重试；恢复后重新参与调度；
  - 回滚不影响其他目标；成功路径清零失败计数。
- 单测与服务日志均打印目标、计划与判定依据（`[submit]`/`[dispatch]`/`[remediate]`/
  `[rollback]`/`[execute]` 前缀，排队场景额外打印排队位置与出队重判结论），
  可直接从日志核对每次判定。
