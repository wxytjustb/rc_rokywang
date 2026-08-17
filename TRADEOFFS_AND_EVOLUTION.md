# 取舍与演进总结

## 1. 总体演进

整个设计过程可以概括为：

> 方案从一个“通用企业通知平台”，逐步收敛成了一个“支持多供应商、但每条消息只投递一个供应商的稳定可靠投递器”。

设计目标不再是覆盖尽可能多的通知和编排场景，而是用清晰的业务边界降低数据模型、协议、可靠性和长期维护的复杂度。

## 2. AI 的方案方式与用户输入

### 2.1 AI 的方式

AI 最初采用的是“先覆盖通用企业通知平台能力，再逐步裁剪”的方式：

- 从可靠投递问题出发，优先考虑异步受理、持久化、重试、幂等、死信和可观测性。
- 在业务边界尚未完全明确时，按可复用平台建模，引入路由、端点、任务、尝试记录、Outbox 和配置版本等抽象。
- 按未来规模设计 PostgreSQL、Redis、消息队列、供应商隔离和弹性扩容等演进路径。
- 用 `provider adapter` 隔离不同供应商的 URL、Header、Body、认证和响应判断。
- 曾按“成功、临时失败、永久失败、结果不确定”设计 Provider `Outcome` 和 `SendWithClassifier`，以降低非幂等动作重复执行的风险。
- 在用户持续明确范围后，逐步删除没有现实需求支撑的抽象和字段。

这种方式的价值是较早暴露了可靠性、外部幂等和故障恢复边界；不足是过早按通用平台和未来规模展开，在当前稳定、单用途系统中产生了不必要的表、协议字段和运维组件。

### 2.2 用户输入形成的决策台账

| 主题 | AI 初始方式 | 用户输入的决策 | 最终结果 |
|---|---|---|---|
| 系统定位 | 按通用通知投递平台设计 | 基础业务系统应稳定运行，不应频繁升级和变化 | 收敛为稳定可靠投递器，不承担通用编排 |
| 供应商范围 | 一条事件可以路由和扇出到多个供应商 | 系统支持多个供应商，但每条消息只投递一个供应商 | 一行记录、一次外部副作用，不处理聚合结果和跨供应商事务 |
| 数据模型 | 拆分 Event、Route、Endpoint、Task、Attempt、Outbox、Dead Letter | 只保留标准事件表，相关状态合并 | 使用单表 `notification_event` |
| 消息分发 | 讨论 PostgreSQL 扫描、Redis 队列以及专用 MQ | 轻量部署不应强制依赖外部 MQ，高吞吐部署仍需消息队列 | PostgreSQL 保存事实；`memory` 用进程内 Channel 唤醒，NSQ/RabbitMQ 负责跨进程分发 |
| 业务扩展 | 使用 `message_type`、Schema 和路由配置表达变化 | 增加 `provider_action`，删除 `message_type` 和 `schema_version` | `provider_code + provider_action` 决定校验和适配逻辑 |
| 业务幂等 ID | 使用通用 `external_event_id`，后改为 `request_id` | 改为与 `source_system` 对应的 `source_request_id` | `source_system + source_request_id` 构成唯一内部幂等键 |
| MQ 协议 | MQ Envelope 包含独立 `message_id` 和上下文 | `message_id` 与 `event_id` 重复 | MQ 消息体只保留 `event_id` |
| Worker 重试 | Worker 根据 Provider 分类维护自己的重试分支 | 去掉 Worker 自身重试循环，复用 MQ 的 requeue | Worker 返回 typed requeue directive；数据库记录真实调用次数，Consumer 执行精确延期 |
| 重试状态 | 失败后恢复为 `PENDING` 或按结果进入不同终态 | requeue 期间 `notification_event` 本身仍是 `PROCESSING` | 仅清空/更新租约并记录 `REQUEUE_REQUESTED`；下次领取递增 `attempt_count` |
| MQ 重试配置 | 各 MQ 使用不同默认机制 | Memory、NSQ、RabbitMQ 统一增加 `max_attempts` 和线性延时规则 | 延时按数据库真实供应商调用次数计算；Broker attempts 只用于观测 |
| Provider 结果 | 用 `Outcome` 和 `SendWithClassifier` 区分临时、永久和不确定结果 | 用户认为分类过度设计，要求只要报错就统一 requeue | `Result` 只表达是否明确成功；所有未成功结果重试，次数耗尽后 `FAILED` |
| 外部幂等 | 优先建议向供应商传递 `Idempotency-Key` | 无法保证外部供应商支持该能力 | 内部幂等与外部结果分离；未明确成功统一重试并接受重复风险 |
| API Token | Token 与 `source_system` 绑定或按来源分别配置 | Token 不再基于 `source_system` | 配置了 `auth.tokens` 就直接使用；未配置时启动自动生成并通过日志输出 |
| 结果保存 | 最初只记录内部投递结果 | 增加供应商请求返回结果字段 | `last_result` 保存内部状态，`provider_response` 保存脱敏后的供应商响应 |
| 结果查询 | 最初侧重异步投递和运维查询 | 增加基于 `source_request_id` 的处理结果接口 | 调用方按自己的请求 ID 查询，并显式提供 `source_system` 组成完整查询键 |

这组决策体现出的共同原则是：先明确业务边界和命名，再保留支撑可靠投递所必需的最小机制；尚未发生的复杂需求只记录为演进触发条件。

## 3. 初始方案中过度设计的部分

| AI初始设计 | 过度设计原因 | 最终取舍 |
|---|---|---|
| 一条事件可以路由到多个供应商 | 引入部分成功、跨供应商一致性和补偿问题，使用分布式事务增加系统的复杂性和不稳定性 | 一条消息只能指定一个供应商 |
| 独立的 Event、Route、Endpoint、Task、Attempt、Outbox、Dead Letter 表 | 当前业务关系简单，表之间状态同步和维护成本过高 | 合并为一张 `notification_event` |
| 动态路由、Endpoint 版本、模板版本 | 系统是稳定基础设施，不需要高频调整和动态编排 | 供应商配置放在受控配置文件或配置中心 |
| 通用供应商插件和复杂控制面 | 超出了“稳定投递”的核心目标 | 使用固定的 Provider Adapter Registry |
| 保存完整投递尝试历史 | 增加表、存储和查询复杂度 | 只保存最后一次 `last_result` 和 `provider_response` |
| 独立 Outbox 表 | 对单表模型而言增加数据库双写和状态同步 | `notification_event` 通过 `status + enqueued_at` 兼任简化 Outbox |
| 独立死信表 | 当前只需表达最终处理结果 | 达到 `max_attempts` 后使用 `status=FAILED`；`UNKNOWN` 仅兼容历史数据 |
| Redis 同时承担数据库、队列和调度 | 持久化、延迟重试、审计和恢复逻辑反而更复杂 | PostgreSQL 保存事实，MQ 负责分发；Redis 只可选用于限流 |
| 完整标准事件元数据 | `message_type`、`schema_version` 与当前动作模型重复 | 由 `provider_code + provider_action` 决定协议 |
| MQ 中包含完整上下文 | 会形成数据库与 MQ 两份事实 | MQ 只传 `event_id` |
| MQ 独立 `message_id` | `event_id` 已经唯一，Worker 也按它领取任务 | 删除 `message_id` |
| Worker 内部重试循环 | 与 NSQ/RabbitMQ/Memory 自带的 ACK/requeue 生命周期重复，容易形成两套次数和延时 | Worker 单次只执行一次 Provider 调用，失败交回 MQ |
| Provider 四类 `Outcome` 与 `SendWithClassifier` | 当前明确选择“优先给发送更多机会”，四类结果并不改变处理动作 | 删除分类器；只保留明确成功与未成功两种语义 |
| 假设供应商支持 `Idempotency-Key` | 外部供应商能力不受本系统控制 | 内部保证幂等，外部不承诺恰好一次 |

这些设计并非在所有场景下都错误，但它们更适合通用平台、复杂路由、多租户和大规模编排，超出了当前系统的真实边界。

## 4. 最终方案的关键设计决策

### 4.1 多供应商，但单消息单供应商

系统整体支持多个供应商：

```text
消息 A → CRM
消息 B → 库存
消息 C → 广告
```

但一条消息不能同时发送给多个供应商。

这个决策消除了：

- 部分成功。
- 跨供应商事务。
- 补偿编排。
- 多投递任务聚合状态。
- 一条事件对应多个 Delivery Task。

如果一个业务事实确实需要通知两个供应商，来源系统应提交两个具有不同 `source_request_id` 的独立消息。

### 4.2 `source_system + source_request_id` 是内部幂等键

最终标识关系为：

```text
source_system
+ source_request_id
= 唯一业务请求
```

相比通用的 `request_id`，`source_request_id` 明确表达了这个 ID 来自业务系统，也与 `source_system` 形成自然对应。

唯一约束故意不包含供应商：

```sql
UNIQUE (source_system, source_request_id)
```

因此，同一个请求不能先发送给供应商 A，再改成供应商 B。

### 4.3 `provider_code + provider_action` 决定处理逻辑

例如：

```text
crm-vendor-a + update_contact_status
inventory-vendor-b + set_inventory
advertisement-vendor-c + report_registration
```

这个组合负责确定：

- Payload 校验规则。
- HTTP Method 和 URL。
- Header 与 Body 转换。
- 认证方式。
- 如何判断供应商明确成功并记录错误代码与摘要。

配置层不再用一个全局 `ProviderConfig/ActionConfig` Schema 假设所有供应商相同。`providers` 根节点只按 `provider_code` 保存原始 YAML；对应 Adapter 的 `Config` 方法使用自己的私有结构严格解码，并把结果归一化为 Worker 运行参数。HTTP Method、认证字段、动作名、成功响应格式和可选熔断参数由各 Adapter 的规则决定，跨 Adapter 字段或拼错字段会在启动时失败；普通重试上限和延时由 MQ 配置提供。

因此不再需要额外的：

```text
message_type
schema_version
route_id
endpoint_id
```

如果 Payload 出现不兼容变化，不修改原有动作语义，而是增加新动作：

```text
update_contact_status
update_contact_status_v2
```

### 4.4 单表表达完整投递状态

`notification_event` 同时承担：

- 标准业务事件。
- 投递任务。
- 简化 Outbox。
- 重试状态。
- 死信状态。
- Worker 租约。
- 最新处理结果。
- 最新供应商响应。

这是本次设计最重要的简化决策。

明确接受的代价包括：

- 不保存完整尝试历史。
- 不提供复杂审计模型。
- 不支持一条消息多目标。
- 不适合动态工作流。
- 历史供应商配置不在数据库中完整还原。

### 4.5 PostgreSQL 保存事实，MQ 只负责通知

最终 MQ 消息只有：

```json
{
  "event_id": "64b19467-fc14-4858-91f8-042e8c78eec8"
}
```

所有业务字段从 PostgreSQL 读取。

这样做的取舍是：

- 优点：MQ 消息极小。
- 优点：不会有两份 Payload 不一致。
- 优点：MQ 产品可以替换。
- 优点：重复消息可以通过数据库状态消化。
- 代价：Worker 每次消费需要执行一次数据库主键点查。

目前这个点查成本是可接受的。只有实际监控证明 PostgreSQL 读取成为瓶颈后，才考虑增加缓存或扩展 MQ 消息。

轻量部署可以选择 `mq.driver=memory`：API 与 Worker 在同一进程中，通过有缓冲 Channel 传递 `event_id`。Channel 不具备持久化能力，因此不会更新 `enqueued_at`；进程重启或队列满载后的恢复仍由 PostgreSQL 补偿扫描保证。需要 API 与 Worker 独立部署或扩容时，应切换到 NSQ/RabbitMQ。

### 4.6 不假设外部供应商具备幂等能力

内部幂等只能阻止业务系统重复提交，不能阻止供应商重复处理。

在以下场景中：

```text
供应商已经执行成功
→ 响应返回途中断开
→ Worker只观察到超时
```

系统无法同时保证“不丢失”和“不重复”。

当前决策是统一选择重试机会：只有供应商明确成功才 ACK，其余错误按数据库真实调用 `max_attempts` 和统一延时公式 requeue。这样降低漏送概率，但非幂等动作可能重复执行。系统仍不宣称外部恰好一次，也不把 `source_request_id` 等同于供应商幂等能力。

Provider 层因此不再输出临时、永久或不确定等 `Outcome`。Adapter 返回的 `Result` 表达 `Success`、仅供熔断使用的 `AvailabilityFailure`、HTTP 状态和诊断信息；通用 `Send` 只负责执行 HTTP 请求并保存受限、脱敏后的响应。以 Lark Bot 为例，只有 HTTP 200 且响应 `code=0` 才是明确成功，`code=19022`、其他非零业务码、HTTP 错误、响应解析错误和网络错误都进入同一 requeue 路径，但只有可用性失败影响熔断计数。

### 4.7 区分内部结果和供应商响应

最终保留两个字段：

```text
last_result
provider_response
```

职责分别是：

- `last_result`：内部执行阶段、错误分类、尝试次数、重试判断和耗时。
- `provider_response`：供应商明确返回的、经过脱敏和截断的 HTTP 结果。

这样业务状态与外部响应不会混在一起。

### 4.8 基于业务 ID 查询结果

查询接口使用：

```http
GET /v1/messages/{source_request_id}
```

Token 只验证 API 访问权限，不再绑定来源系统。调用方通过 Query 参数提供 `source_system`，实际查询条件是：

```text
source_system
+ source_request_id
```

这样业务系统不必保存内部 `event_id`；当前方案不提供来源系统级权限隔离。

### 4.9 Token 配置只负责 API 访问

`auth.tokens` 是服务级 Bearer Token 列表，不再根据 `source_system` 查找或派生 Token：

- 配置了一个或多个 Token 时，服务直接使用配置值。
- 没有配置有效 Token 时，服务在启动时自动生成一个，并通过日志输出，便于本地或首次部署使用。
- `source_system` 仍是业务幂等键和结果查询条件的一部分，但不代表鉴权身份。

这个决策简化了配置和调用方式；接受的代价是当前只提供服务级访问控制，不提供来源系统级权限隔离。生产环境应显式配置稳定 Token，避免重启后自动生成值变化。

### 4.10 MQ 统一承担 ACK 与 requeue，数据库记录真实调用次数

当前链路是标准的生产者—消费者模式：API/补偿器发布 `event_id`，Worker 通过所选 MQ Consumer 领取。Worker 对一次 Delivery 只调用一次 Provider，不在内部 sleep 或循环重试：

```text
明确成功 → 更新 SUCCEEDED → Handler 返回 nil → ACK/FIN
未明确成功且未耗尽 → 保持 PROCESSING → Handler 返回 error → MQ requeue
未明确成功且已耗尽 → 更新 FAILED → Handler 返回 nil → ACK/FIN
```

普通 Provider 失败统一使用：

```text
delay = min(default_requeue_delay × attempt_count, max_requeue_delay)
```

`max_attempts` 包含首次真实供应商调用，`0` 表示不限次数。`notification_event.attempt_count` 是终态判断的权威口径；MQ attempts 可以递增但只用于日志。普通 requeue 不把事件改回 `PENDING`，只清空当前 Worker 租约、保存恢复期限和 `REQUEUE_REQUESTED`；下一次 MQ 投递原子取得新租约并递增 `attempt_count`。如果崩溃恢复后的 Claim 使计数大于上限，Processor 会在 Provider 调用前终止，并回退这次未发生调用的计数增量。

Action 可选启用进程内熔断。熔断拒绝发生在 Provider 调用、限流和 `REQUESTING` 之前，Store 会原子回退本次领取增加的 `attempt_count`，因此延期不会耗尽真实调用次数。熔断开放使用剩余开放时间精确延期；半开探测正忙时至少等待默认间隔。NSQ 对这种延期使用无 Consumer 全局 backoff 的 requeue，普通 Provider 失败继续使用带 backoff 的 requeue。

进程重启后能够重新发送并不是 Worker 保存了内部重试任务，而是 PostgreSQL 仍保存事实：启动时的补偿扫描会重新发布尚未成功首次入队的 `PENDING` 事件，也会在租约到期后恢复丢失 MQ 唤醒的 `PROCESSING + REQUEUE_REQUESTED` 事件。Worker 在外部请求期间崩溃时同样进入 requeue；如果请求实际已经被供应商处理，这条恢复路径也可能造成重复发送。

## 5. 最终方案的核心取舍

| 得到的收益 | 接受的代价 |
|---|---|
| 数据库只有一张核心表 | 没有完整尝试历史 |
| 一条消息只有一个状态 | 不支持广播和聚合结果 |
| MQ 协议极简 | Worker 需要数据库点查 |
| `memory` 模式无需外部 Broker | Worker 与 API 同进程，Channel 不持久化 |
| 配置不进入复杂关系表 | 历史配置重现能力有限 |
| 不依赖供应商幂等能力 | 未确认结果重试时可能产生重复副作用 |
| 不使用协议版本字段 | 不兼容变化需要新增 `provider_action` |
| 不使用分布式事务 | 发布和消费采用至少一次语义 |
| 重试统一交给 MQ | Worker 没有独立重试循环，但运行必须依赖 Consumer 正确实现 ACK/requeue |
| 所有未成功结果统一 requeue | 配置简单、失败有更多恢复机会，但永久配置错误也会重试到上限 |
| Action 级进程内熔断 | 不增加外部状态，但重启后恢复关闭且多个副本会各自探测 |
| 系统职责稳定明确 | 不适合作为通用事件编排平台 |

## 6. 推荐演进原则

当前设计不应预先增加更多表和组件。只有出现已经被监控证明的实际问题时再演进。

### 6.1 PostgreSQL 发布扫描成为瓶颈

再考虑：

- CDC/WAL 订阅。
- 独立 Outbox 表。
- 分区表。
- 批量发布优化。

### 6.2 需要完整投递历史

再增加：

```text
notification_attempt
```

现阶段不提前建设。

### 6.3 单个供应商影响其他供应商

当前已按 Action 提供进程内熔断；如果仍出现多个供应商相互占用 Worker，再增加：

- 按供应商划分 Queue。
- 独立 Worker Pool。
- 跨副本共享的供应商熔断状态（仅在重复探测成为实际问题时）。
- Redis 分布式限流。

### 6.4 需要一条消息投递多个供应商

不建议直接扩展当前单表状态机，而应把它视为新的“消息编排系统”需求，单独设计父事件、子任务、聚合状态和补偿策略。

### 6.5 Payload 出现不兼容变化

优先新增：

```text
provider_action_v2
```

不修改现有 Action 的字段语义。

### 6.6 重复副作用变得不可接受

只有当实际业务证明非幂等 Provider 的重复发送代价不可接受时，再考虑供应商原生幂等键、发送结果查询/对账，或为特定 Action 引入经过明确评审的重试策略。不要重新引入全局四分类 `Outcome`，除非不同分类确实对应不同且可验证的处理动作。

## 7. 结论

最终设计思想可以总结为：

> 用业务边界消除技术复杂性，而不是先建设一个通用平台，再用复杂机制约束它。

系统追求的不是能力最多，而是：

```text
职责单一
状态明确
协议最小
可靠性边界诚实
可长期稳定运行
需要时再演进
```

在当前可靠性策略下，“状态明确”具体意味着：只有供应商明确成功才结束投递，其他错误统一交给 MQ 提供有限或无限的重试机会；系统同时明确接受外部非幂等操作可能重复执行的代价。
