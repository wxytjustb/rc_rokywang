# Memory 单进程端到端压测与性能调试方案

## 总结

- 测量范围定义为：`POST /v1/messages` 受理开始，到 `notification_event` 成功写入终态为止；主指标是端到端吞吐、P95/P99 时延、积压量和恢复时间。
- 采用 Memory 单进程拓扑、独立 PostgreSQL、回环地址上的供应商模拟器，不调用真实飞书接口；结论不外推到 RabbitMQ/NSQ 或多副本部署。
- 当前 `lark-bot/send` 配置为 `requests_per_second=5`、`max_concurrency=5`，Worker 并发为 10，因此供应商延迟低于 1 秒时，预期持续吞吐上限首先受 5 次/秒限流约束，而不是 CPU。理论上限近似为：

  `min(5 RPS, 5 / 单次供应商耗时秒数, 10 / 单次供应商耗时秒数)`

## 观测与接口改造

- 增加独立诊断监听端口，提供 `/metrics` 和可开关的 `/debug/pprof/*`；默认关闭、不挂载到业务 Gin 路由、不暴露到公网。新增 `observability.enabled`、`observability.addr`、`observability.pprof_enabled` 配置，业务 API 和数据库结构保持不变。
- 使用独立 Prometheus Registry，注册 Go Runtime、进程、数据库连接池以及以下低基数业务指标：
  - 终态事件数和结果：`delivery_events_total{provider,action,outcome}`。
  - `created_at` 到终态数据库更新完成：`delivery_e2e_duration_seconds`。
  - 限流等待、数据库 Claim、供应商调用、终态更新等阶段耗时。
  - Worker 活跃数、Memory Channel 深度/容量、队列满次数、延迟重入队次数。
  - Claim 成功/未领取次数，用于识别补偿扫描产生的重复唤醒。
  - `PENDING`、`PROCESSING` 数量及最老待处理事件年龄。
  - `sql.DB` 的 open/in-use/idle/wait-count/wait-duration。
- 指标标签禁止加入 `event_id`、`source_request_id` 等高基数字段；端到端时延在终态更新成功后记录，压测脚本不逐条轮询 GET 接口，避免查询流量污染结果。
- 保持 EventRepo 的条件更新、租约和原子状态转换不变；性能优化只能基于实际 SQL 证据，不能改成非原子 ORM CRUD。

## 工具与测试设施

| 用途 | 工具 | 使用方式 |
|---|---|---|
| 负载生成 | Grafana k6 | 使用开放模型 `constant-arrival-rate`，生成唯一幂等键，只提交 POST 并校验 202；固定到达率不会因系统变慢而自动降低压力。[k6 官方说明](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/) |
| 指标与趋势 | Prometheus + Grafana | 统一展示 k6、Go、Worker、Memory Channel 和数据库指标；k6 可通过 Prometheus Remote Write 输出。[Prometheus Go 指标指南](https://prometheus.io/docs/guides/go-application/)、[k6 Remote Write](https://grafana.com/docs/k6/latest/results-output/real-time/prometheus-remote-write/) |
| 供应商模拟 | 小型 Go HTTP Stub | 实现飞书 Webhook 路径和响应格式，按请求 ID 确定性返回成功、首次 429、5xx、慢响应或超时，并统计不同业务 ID 的实际调用次数。 |
| 网络故障 | Toxiproxy | 只在超时、连接中断、延迟和带宽实验中启用；正常容量基线绕过代理。[Toxiproxy 官方仓库](https://github.com/shopify/toxiproxy) |
| Go 根因定位 | `go tool pprof` | 稳态高负载下采集 30 秒 CPU；Soak 前后采集 Heap；仅在怀疑锁或调度问题时单独启用 mutex、block、goroutine Profile。[Go Diagnostics](https://go.dev/doc/diagnostics) |
| 数据库定位 | `pg_stat_statements`、`EXPLAIN (ANALYZE, BUFFERS)` | 找出总耗时、调用次数和平均耗时最高的 SQL，再对具体 Claim、扫描或终态更新分析执行计划。[pg_stat_statements](https://www.postgresql.org/docs/16/pgstatstatements.html)、[EXPLAIN](https://www.postgresql.org/docs/current/using-explain.html) |
| 调度深挖 | `go tool trace` | 仅当 CPU 不高但吞吐停滞、goroutine 大量等待时使用，检查调度、GC、系统调用和串行化。 |

## 压测场景与判定

1. 固定 release 构建、CPU/内存配额、数据库连接池、Worker 并发、日志等级和模拟器延迟；正式负载生成器运行在被测主机之外。每个容量场景使用干净数据库并保存完整配置快照。
2. 成功路径模拟器固定耗时 100ms、错误率为零：
   - Smoke：1 RPS，2 分钟。
   - 阶梯：2、4、5、6、8、10 RPS，各 10 分钟，场景间停止流量并等待完全排空。
   - 最高稳定档定义为：预热后终态吞吐达到输入量的 98%，测量期净积压不超过提交量的 1%，无 k6 dropped iterations、无意外 `FAILED`。
3. 在最高稳定档的 80% 运行 60 分钟 Soak；检查 P95/P99 漂移、堆增长、GC、goroutine、数据库等待和重复唤醒是否持续增长。
4. 分别将供应商耗时设为 500ms、1500ms，验证吞吐拐点是否符合限流/并发公式，并重点观察 `limiter_wait` 与 `provider_duration`。
5. 饱和与恢复：以稳定档两倍流量运行 2 分钟后停止提交，记录 Channel 满、数据库待发布积压及完全排空时间。
6. 故障场景独立执行：
   - 每十个事件中一个首次返回 429、第二次成功，验证重试压力和最终完成率。
   - 每一百个事件中一个超过 5 秒超时，验证其按 MQ 延时重新调用，并在 `max_attempts` 后进入 `FAILED`。
   - 稳定压力下重启单进程，验证 Memory 唤醒丢失后由 PostgreSQL 补偿恢复，同时不存在重复供应商投递。
7. 首轮不设置主观延迟 SLO，只产出容量拐点。后续回归门槛采用：持续目标流量为最高稳定档的 70%，吞吐下降不得超过 10%，P95/P99 上升不得超过 20%，并保持业务结果一致。
8. 最终报告保存 k6 原始结果、Grafana 快照、关键 pprof、Top SQL、测试配置与资源规格，并明确区分“限流导致的设计上限”和“CPU、内存、数据库或队列导致的性能瓶颈”。

## 假设与边界

- 使用现有 Memory Buffer 1024、Worker 并发 10、供应商 5 RPS/并发 5 的默认参数。
- 模拟器监听 `127.0.0.1`，满足当前适配器仅允许回环 HTTP 测试地址的安全校验。
- 正式基线采用生产等价 INFO 日志；额外执行一次 WARN 日志对照实验，量化同步 JSON 访问日志的开销。
- 当前未给出生产数据保留量，因此首轮不声称覆盖大表老化或长期表膨胀；取得预计保留量后再增加等量历史终态数据场景。
