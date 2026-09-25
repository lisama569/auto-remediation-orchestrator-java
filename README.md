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
        ┌──────────┴───────────┐
        ▼                      ▼
    REJECTED               PENDING ──▶ RUNNING ──┬─▶ SUCCEEDED   (修复成功，健康校验通过)
   （附带可区分的拒绝原因）                        ├─▶ ROLLED_BACK  (重试耗尽/超时/校验失败，已回滚)
                                                  └─▶ FAILED       (执行链路出现未预期异常)
```

- `REJECTED` / `SUCCEEDED` / `ROLLED_BACK` / `FAILED` 均为终态，稳定且可区分。
- 执行链路中任一步失败都会记录日志并落入对应终态，不会被静默吞掉。

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
| 4 | 同一目标已有进行中的计划 | `PLAN_CONFLICT` |

- 同一目标同一时刻只允许一个进行中的计划：通过 `activePlans` 原子占位实现。
- 幂等键通过 `putIfAbsent` 原子登记；并发下同键竞争失败者释放占位并按
  `DUPLICATE_REQUEST` 处理，绝不造成重复执行。
- 并发下同键请求若先撞上目标占位，可能得到 `PLAN_CONFLICT`——二者均为合法
  且可区分的拒绝，关键不变量（同键只执行一次）始终成立。

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
| POST | `/api/plans` | 下发计划 `{targetId, faultType, idempotencyKey}`；接受 202，拒绝 409（含原因） |
| GET | `/api/plans/{id}` | 查询计划状态 |

## 验证方法

```bash
mvn test
```

- `PlanOrchestratorConcurrencyTest`
  - 16 线程并发下发：恰好 1 个被接受，其余 `PLAN_CONFLICT`；
  - 重复下发（进行中与终态后）：均判定 `DUPLICATE_REQUEST` 且返回原计划；
  - 同键并发：恰好执行一次，重放仍返回原计划；
  - 隔离/维护窗口拒绝原因可区分。
- `RemediationEngineTest`
  - 动作超时（HANG）→ `ROLLED_BACK`，目标恢复执行前状态、无中间态；
  - 健康校验未通过（NOOP）→ `ROLLED_BACK`；
  - 连续失败达阈值 → 隔离、停止重试；恢复后重新参与调度；
  - 回滚不影响其他目标；成功路径清零失败计数。
- 单测与服务日志均打印目标、计划与判定依据（`[submit]`/`[remediate]`/
  `[rollback]`/`[execute]` 前缀），可直接从日志核对每次判定。
