# 本地故障注入与自动修复编排服务

一个**纯进程内**的故障注入（Fault Injection）与自动修复（Auto-Remediation）编排服务。
被编排的"实例"由内存中的目标状态模拟，不触碰任何真实系统资源；仅依赖本地 JDK 21
与工程已有的 Spring Boot 依赖。设计目标是让并发触发与修复失败时的判定**一致、
可复现、可解释**。

## 1. 总体结构

```
domain/   目标状态机、计划、策略配置、拒绝/修复结果枚举
engine/   目标注册表(per-target 锁)、超时执行器、时钟、审计日志
service/  PlanService（计划下发/注入）、RepairService（修复/退避/隔离/回滚）、Orchestrator（门面）
web/      薄 REST 门面（仅参数绑定与状态码映射）
```

- `TargetRegistry`：为每个目标维护一把独立监视器锁；同一目标的判定串行，不同目标互不阻塞。
- `Clock`：时间源抽象。生产用 `SystemClock`，测试用 `MutableClock`，使维护窗口与退避判定完全确定。
- `ActionExecutor`：每个动作在独立虚拟线程执行，调用方以 `Future.get(timeout)` 限时等待，超时即中断。
- `AuditLog`：每一次判定（接受/拒绝/执行/回滚/计数/隔离/恢复）同时写入内存列表与 SLF4J，
  日志包含**目标、计划、动作、结果与判定依据**，测试可断言、运行可解释。

## 2. 目标状态机

```
            注入成功                     修复成功（探针通过）
 HEALTHY ───────────────▶ UNHEALTHY ───────────────▶ HEALTHY
   ▲                         │  │                         ▲
   │                         │  │ 连续修复失败达到阈值      │
   │      recover() 探针通过 │  ▼                         │
   └─────────────────────────┴ QUARANTINED ───────────────┘
```

- **HEALTHY**：初始态，无生效故障（`activeFault == null`）。
- **UNHEALTHY**：故障注入成功后的故障态，携带故障标记（如 `DISK_FULL`）。
- **QUARANTINED**：连续修复失败达到阈值后的隔离态；停止继续重试，计划下发被拒。
  调用 `recover(targetId)` 且确认故障已消除后，脱离隔离、计数清零，重新参与调度。

计划状态机（持目标锁推进）：`PENDING → RUNNING → COMPLETED | FAILED`。

## 3. 计划下发：拒绝原因（稳定且可区分）

`POST /api/plans`，在目标锁内按固定顺序判定，任一不满足即拒绝，拒绝原因通过
`SubmitResult.rejectReason`（HTTP 409）明确区分：

| 顺序 | RejectReason | 触发条件 |
|----|--------------|----------|
| 1 | `DUPLICATE_REQUEST` | 幂等键 `idempotencyKey` 已被接受（重复下发不重复执行） |
| 2 | `OUTSIDE_MAINTENANCE_WINDOW` | 当前时间不在 `[windowStartMs, windowEndMs)` |
| 3 | `TARGET_QUARANTINED` | 目标处于隔离态 |
| 4 | `PLAN_ALREADY_IN_PROGRESS` | 同一目标已有进行中的计划（`activePlanId != null`） |

> 注意：只有**通过全部前置检查并被接受**的请求才会占用幂等键。因此一个因窗口外
> 被拒的请求，待进入窗口后用同一键重试不会被误判为重复。

### 并发模型（关键）

同一时刻同一目标只允许一个进行中的计划。目标锁保护两个短临界区：

1. **检查 + 占位**：校验通过后置 `activePlanId = planId`（claim）；
2. **终态推进 + 回滚**：动作结束后置目标状态、结束/失败计划、释放占位。

**注入动作本身在锁外执行**。占位期间任何并发请求进入临界区都会被*立即*拒绝为
`PLAN_ALREADY_IN_PROGRESS`（而不是排队等待），因此既不会有两个计划并发执行，
也不会出现重复执行。动作结束后排队到来的请求作为后续新计划串行受理。

## 4. 自动修复策略语义

`POST /api/targets/{id}/repair` 触发一次修复，在目标锁内做前置判定：

| RepairOutcome | 含义 | 是否计数 |
|---------------|------|----------|
| `ALREADY_HEALTHY` | 目标健康，无需修复 | 否 |
| `ALREADY_QUARANTINED` | 已隔离，停止重试 | 否 |
| `ALREADY_RUNNING` | 已有修复在执行（占位期间） | 否 |
| `PLAN_IN_PROGRESS` | 有进行中的注入计划 | 否 |
| `BACKOFF_SKIPPED` | 仍在退避窗口 `now < nextEligibleAtMs` | 否 |
| `TIMED_OUT` / `FAILED` / `HEALTH_CHECK_FAILED` | 真正执行过动作的失败 | **是（+1）** |
| `REPAIRED` | 动作成功且健康探针通过 | 计数归零 |
| `QUARANTINED` | 本次失败后连续失败达到阈值 | 进入隔离 |

- **计数不重不漏**：失败计数发生在动作结束后的终态临界区（先回滚、后 +1），
  由目标锁串行化；跳过分支一律不计数。
- **指数退避**：第 n 次失败后退避 `backoffBaseMs * backoffMultiplier^(n-1)`，
  上限 `backoffMaxMs`（默认 100ms 起步、2 倍、上限 5s）。
- **隔离阈值**：`consecutiveFailures >= maxConsecutiveFailures`（默认 3）即隔离并停止重试。
- **修复执行**：执行修复动作（限时 `repairTimeoutMs`）→ 健康探针 `HealthProbe` 校验。
  默认修复动作清除故障标记，默认探针要求无生效故障且状态健康；两者均可在构造
  `Orchestrator` 时替换，以模拟异常、阻塞超时与探针失败。

## 5. 回滚规则

修复或注入前保存目标完整状态快照（`Target.Snapshot`：状态、连续失败数、
activePlanId、repairRunning、退避截止时间、activeFault）。

- **触发回滚**：动作超时、动作抛异常、健康校验未通过（或探针自身异常）。
- **回滚语义**：精确恢复到执行前快照，因此不会留下 `activeFault`、`repairRunning`、
  `activePlanId` 等中间态；回滚只作用于本目标的聚合根，**不影响其他目标**。
- **计数与回滚的顺序**：失败路径"先恢复快照、再 +1"，保证计数恰好一次且不被回滚撤销。
- **失败不静默**：任一步异常都会体现在审计日志中；回滚本身若失败会抛出
  `RollbackException` 向上传播，绝不被吞掉。超时由 `TimeoutRuntimeException`
  与普通动作异常区分，结果中分别为 `TIMED_OUT` 与 `FAILED`。

## 6. REST 接口

| 方法与路径 | 说明 |
|------------|------|
| `POST /api/targets/{id}` | 注册目标（幂等） |
| `GET  /api/targets/{id}` | 查询目标状态视图 |
| `POST /api/plans` | 下发计划；成功 200，拒绝 409 且带 `rejectReason` |
| `POST /api/targets/{id}/repair` | 触发一次修复，返回 `RepairResult` |
| `POST /api/targets/{id}/recover` | 确认隔离目标恢复，重新参与调度 |
| `GET  /api/audit` | 查询全部审计事件（判定依据） |

计划请求体示例：

```json
{
  "planId": "plan-001",
  "targetId": "vm-1",
  "faultType": "DISK_FULL",
  "idempotencyKey": 1700000000001,
  "windowStartMs": 1700000000000,
  "windowEndMs": 1700003600000
}
```

## 7. 验证方法

```bash
mvn test
```

覆盖的场景（共 19 个测试，全部确定性、无固定 sleep 等待墙钟）：

- **计划下发**（`PlanSubmissionTest`）：窗口外、隔离、重复幂等键三类拒绝原因；
  成功注入终态；注入失败回滚。
- **并发触发**（`ConcurrentPlanTest`）：进行中期间 6 个竞争请求全部
  `PLAN_ALREADY_IN_PROGRESS`，注入动作恰好执行一次；排队请求串行受理且
  "受理数 == 动作执行数"。
- **重复下发**：同幂等键第二次返回 `DUPLICATE_REQUEST`，动作只执行一次。
- **修复策略**（`RepairServiceTest`）：失败计数、指数退避窗口内 `BACKOFF_SKIPPED`
  不计数、阈值隔离、隔离后恢复重新调度、健康校验失败回滚、失败后成功计数归零。
- **并发修复**（`ConcurrentRepairTest`）：修复进行中 11 个并发触发全部
  `ALREADY_RUNNING` 且不计数，最终失败计数恰好为 1。
- **超时回滚**（`TimeoutRollbackTest`）：修复/注入超时在超时点（~150ms）回滚到
  执行前状态，无任何中间态残留。
- **多目标隔离**（`TargetIsolationTest`）：一个目标隔离/回滚不影响其他目标。
- **REST 冒烟**（`OrchestratorWebTest`）：HTTP 层拒绝码 409 与可区分 `rejectReason`、
  审计端点。

测试与服务日志均打印目标、计划与判定依据，例如：

```
[AUDIT] t=1000 target=vm-q plan=- action=REPAIR outcome=QUARANTINED
        reason=consecutiveFailures=3>=3; rolled back; retries stopped; ...
[TEST] rival 3 accepted=false reason=PLAN_ALREADY_IN_PROGRESS
```

## 8. 默认策略

`Orchestrator.defaultPolicy()`：连续失败阈值 3、退避 100ms×2^(n-1)（上限 5s）、
修复/注入动作超时 2s。故障类型支持 `DISK_FULL`、`NETWORK_BLACKHOLE`、`CPU_BURN`，
默认注入/修复动作只改进程内标记，可通过 `Orchestrator` 构造函数替换以便测试。
