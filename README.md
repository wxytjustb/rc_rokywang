<div align="center">
  <a href="https://github.com/wxytjustb/gopher-post">
    <img src="assets/gopherpost-logo.png" alt="GopherPost logo" width="240">
  </a>

  <h1>GopherPost</h1>
  <p>Reliable External Notification Delivery</p>

  <p>
    <img src="https://img.shields.io/badge/status-early_development-orange" alt="Project status: early development">
    <img src="https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&amp;logoColor=white" alt="Go 1.25">
    <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&amp;logoColor=white" alt="PostgreSQL 16">
    <img src="https://img.shields.io/badge/MQ-RabbitMQ%20%7C%20NSQ%20%7C%20Memory-FF6600" alt="MQ: RabbitMQ, NSQ or Memory">
  </p>
</div>

一个使用 Go 构建的可靠外部通知投递服务。业务系统提交一条消息后，服务先将其持久化到 PostgreSQL，再通过 Worker 异步投递给指定的外部供应商。

项目坚持“单消息、单供应商、单动作”的边界：它专注于稳定投递、幂等受理和结果查询，不提供广播、通用工作流或动态脚本执行能力。

> 当前状态：项目处于早期开发阶段，API、配置格式和数据库结构仍可能变化。当前内置 `lark-bot/send`、`smtp-email/send`、`webhook/deliver` 和 `firebase-push/send`；默认 `providers.yaml` 只启用飞书，另外三个适配器需要配置部署方自己的目标、项目或凭据。

## 特性

- 使用 `source_system + source_request_id` 保证业务请求的内部幂等性。
- PostgreSQL 作为消息状态的唯一事实来源，MQ 消息仅携带 `event_id`。
- 支持 RabbitMQ、NSQ，以及适合单进程部署的内存 Channel。
- 通过数据库条件更新和 Worker 租约处理重复消费与进程崩溃恢复。
- 按 `provider_code + provider_action` 实施 Payload 校验、进程内熔断、并发限制、速率限制和明确成功判断。
- 供应商适配器拥有各自的严格 YAML 配置契约，启动时拒绝未知或错配字段。
- 同时提供 REST、MCP Streamable HTTP 和原生 gRPC 接口；三种协议复用同一业务服务、幂等语义和 Bearer 鉴权。
- 提供消息提交、状态查询、能力发现、存活检查、就绪检查和可选 Swagger UI。
- 对供应商响应进行大小限制、Header 白名单过滤和敏感字段脱敏。

## 架构

```mermaid
flowchart LR
    Client["业务系统 / Agent"] --> REST["REST :8877"]
    Client --> MCP["MCP /mcp :8877"]
    Client --> GRPC["gRPC :8878"]
    REST --> Service["Notification Service"]
    MCP --> Service
    GRPC --> Service
    Service --> DB[("PostgreSQL")]
    Service --> MQ["RabbitMQ / NSQ / Memory"]
    MQ --> Worker["Delivery Worker"]
    Worker --> Adapter["Provider Adapter"]
    Adapter --> Provider["外部供应商 API"]
    Worker --> DB
    Compensator["补偿扫描器"] --> DB
    Compensator --> MQ
```

系统边界、可靠性、失败处理、工程取舍和演进方向见 [设计说明.md](设计说明.md)。方案形成过程和演进触发条件见 [TRADEOFFS_AND_EVOLUTION.md](TRADEOFFS_AND_EVOLUTION.md)。设计文档包含目标方案；当前实现状态以本 README 的“当前限制”和代码为准。

## 技术栈

- Go 1.25
- Gin + Cobra
- gRPC + Protocol Buffers
- Model Context Protocol Go SDK（Streamable HTTP）
- GORM（连接与执行层）
- PostgreSQL 16
- RabbitMQ 3.13、NSQ 1.3 或进程内 Channel
- Swagger / OpenAPI

当前持久化层仅支持 PostgreSQL。`database.driver` 只接受 `postgres`、`postgresql`、`pg` 或空值；配置 MySQL、MariaDB 等其他 Driver 时，Server 和 Worker 会在启动阶段直接失败。GORM 在本项目中用于连接和执行管理，不代表已经实现跨数据库方言兼容。

## 快速开始

### 使用 Docker Compose

需要 Docker 和 Docker Compose。开发编排会启动 API、Worker、PostgreSQL、RabbitMQ，以及备用的 NSQ 开发组件；默认投递链路使用 RabbitMQ。

```bash
docker compose -f dev/docker-compose.yaml up -d --build
```

检查服务状态：

```bash
curl http://127.0.0.1:8877/healthz
curl http://127.0.0.1:8877/readyz
```

Swagger UI 位于 <http://127.0.0.1:8877/docs>，MCP 端点为 <http://127.0.0.1:8877/mcp>，gRPC 监听 `127.0.0.1:8878`。RabbitMQ 管理页面位于 <http://127.0.0.1:15672>，开发账号和密码均为 `test`。

Adminer 数据库管理页面位于 <http://127.0.0.1:8081>，默认连接信息为：

| 项目 | 默认值 |
|---|---|
| 数据库类型 | PostgreSQL |
| 服务器 | `postgres`（Adminer 已自动填写） |
| 数据库 | `test`，由 `POSTGRES_DB` 覆盖 |
| 用户名 | `test`，由 `POSTGRES_USER` 覆盖 |
| 密码 | `test`，由 `POSTGRES_PASSWORD` 覆盖 |

官方 Adminer 镜像不会通过环境变量自动填写用户名和密码，需要在登录页输入。Adminer 只绑定本机地址，默认宿主机端口可通过 `ADMINER_PORT` 覆盖。

停止服务：

```bash
docker compose -f dev/docker-compose.yaml down
```

上述命令不会删除命名数据卷。开发配置中的账号、密码和 Token 仅用于本机测试，不得直接用于生产环境。

### 从源码运行

需要 Go 1.25、PostgreSQL，以及 RabbitMQ 或 NSQ。也可以使用内存模式，只运行 API 进程及其内嵌 Worker。

先启动 PostgreSQL：

```bash
docker compose -f dev/docker-compose.yaml up -d postgres
```

使用内存模式启动服务：

```bash
export LARK_BOT_WEBHOOK_URL='https://open.larksuite.com/open-apis/bot/v2/hook/<real-webhook-id>'
AUTH_TOKEN=dev-system-token MQ_DRIVER=memory SWAGGER_ENABLED=true LOG_LEVEL=debug go run ./cmd/server --config config/server.yaml
```

`AUTH_TOKEN` 未设置或 `auth.tokens` 没有任何非空值时，Server 会在启动时自动生成一个 Bearer Token，并通过日志字段 `token` 打印。Swagger UI 会自动使用实际生效的第一个 Token。

使用外部 MQ 时，分别启动 API 和 Worker：

```bash
export LARK_BOT_WEBHOOK_URL='https://open.larksuite.com/open-apis/bot/v2/hook/<real-webhook-id>'
go run ./cmd/server --config config/server.yaml
go run ./cmd/worker --config config/worker.yaml
```

默认配置选择 RabbitMQ。若从源码运行外部 MQ 模式，请先启动对应 Broker；例如：

```bash
docker compose -f dev/docker-compose.yaml up -d postgres rabbitmq
```

## REST API 使用

下面使用启动示例中配置的 `dev-system-token` 提交一条飞书机器人消息：

```bash
curl -X POST http://127.0.0.1:8877/v1/messages \
  -H 'Authorization: Bearer dev-system-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "source_system": "example-system",
    "source_request_id": "lark-bot-send-request-id-uuid4",
    "provider_code": "lark-bot",
    "provider_action": "send",
    "payload": {
      "msg_type": "text",
      "content": {
        "text": "Notification Delivery test"
      }
    }
  }'
```

使用业务请求 ID 查询结果：

```bash
curl 'http://127.0.0.1:8877/v1/messages/lark-bot-send-request-id-uuid4?source_system=example-system' \
  -H 'Authorization: Bearer dev-system-token'
```

Bearer Token 只用于验证 REST、MCP 和 gRPC 的服务访问权限，不绑定 `source_system`。提交消息时从请求体读取 `source_system`；查询消息时必须提供 `source_system` 和 `source_request_id` 组成的完整幂等键。若要实际发送消息，请通过运行环境中的 `LARK_BOT_WEBHOOK_URL` 提供有效的飞书自定义机器人 Webhook；容器部署时需要把该变量显式加入 Server 和 Worker 的环境配置。不要把真实 Webhook 提交到仓库，仓库内的占位地址只用于配置校验。

`lark-bot/send` 的 `payload` 采用[飞书自定义机器人官方消息体](https://open.larksuite.com/document/ukTMukTMukTM/ucTM5YjL3ETO24yNxkjN)：支持 `text`、`post`、`image`、`share_chat` 和 `interactive`。旧的 `{"text":"..."}` 简写仍可使用，但新接入应使用官方的 `msg_type + content/card` 结构。适配器会在受理阶段检查消息类型和关键字段，并执行官方规定的 20 KB 请求体限制。`LARK_BOT_WEBHOOK_URL` 未配置时服务会启动失败，避免把消息误投到占位地址。

### SMTP 邮件

`smtp-email/send` 发送一封纯文本、HTML 或两者兼有的事务邮件。第一版只接受 `to`、`subject`、`text` 和 `html`，不包含抄送、密送、模板或附件：

```json
{
  "source_system": "order-service",
  "source_request_id": "order-123-email",
  "provider_code": "smtp-email",
  "provider_action": "send",
  "payload": {
    "to": ["user@example.com"],
    "subject": "订单处理完成",
    "text": "订单 123 已处理完成",
    "html": "<p>订单 123 已处理完成</p>"
  }
}
```

适配器支持 `starttls` 和 `implicit_tls`；`disabled` 只允许连接 Loopback 测试服务器。用户名存在时必须同时配置 `password_ref`，密码由 Credential Resolver 注入。邮件只在 SMTP 服务器于 `DATA` 阶段明确接受完整消息后成功；这表示“SMTP 服务器已接受”，不表示邮件已经进入收件箱，也不包含退信回执。相同 `source_system + source_request_id` 的重试使用稳定 `Message-ID`，但这不能保证 SMTP 服务端去重。

### 固定端点 Webhook

`webhook/deliver` 将一个 JSON 对象原样 POST 到启动时配置的固定地址：

```json
{
  "source_system": "order-service",
  "source_request_id": "order-123-webhook",
  "provider_code": "webhook",
  "provider_action": "deliver",
  "payload": {
    "event": "order.completed",
    "order_id": "123"
  }
}
```

目标 URL 不能由 Payload 覆盖，生产地址必须使用 HTTPS，HTTP 只允许 Loopback 测试端点；URL 不允许内嵌凭据、Query 或 Fragment，响应重定向也不会被跟随。认证支持 `none`、`bearer` 和 `hmac_sha256`。所有请求携带由 `provider_code + source_system + source_request_id` 派生的稳定 `Idempotency-Key`；HMAC 模式另外携带 `X-Webhook-Timestamp` 和 `X-Webhook-Signature: sha256=<hex>`，签名内容为 `timestamp + "." + 原始请求体`。只有 HTTP `2xx` 是明确成功。

### Firebase 手机推送

`firebase-push/send` 使用官方 Firebase Admin Go SDK，向一个 Firebase Installation ID（FID，推荐）或兼容期内的 Registration Token 发送跨平台消息。Android 由 FCM 直接投递，iOS 由 FCM 转发到 Firebase 项目配置的 APNs：

```json
{
  "source_system": "order-service",
  "source_request_id": "order-123-mobile-push",
  "provider_code": "firebase-push",
  "provider_action": "send",
  "payload": {
    "fid": "firebase-installation-id",
    "notification": {
      "title": "订单已完成",
      "body": "订单 123 已处理完成",
      "image_url": "https://cdn.example.com/orders/123.png"
    },
    "data": {
      "order_id": "123",
      "screen": "order_detail"
    },
    "android": {
      "priority": "high",
      "channel_id": "orders",
      "sound": "default"
    },
    "ios": {
      "sound": "default",
      "badge": 1,
      "content_available": true
    }
  }
}
```

每个 Payload 必须且只能设置一个 `fid` 或 `token`，不支持 Topic、Condition 或 Multicast，以保持“一条事件、一个外部目标、一个终态”。`notification` 与字符串键值的 `data` 至少提供一个；图片只接受 HTTPS。iOS 图片会自动设置 APNs 的 `mutable-content`，但 App 仍需实现 Notification Service Extension；Android 的 `channel_id` 也必须由 App 预先创建。

Provider 配置需要 Firebase `project_id`。生产环境优先使用 Google Application Default Credentials；也可以配置 `credentials_ref`，由 Credential Resolver 注入完整的 `service_account` JSON。iOS 还必须在 Firebase Console 上传 APNs Authentication Key。FCM 返回非空 message ID 才算成功；这只表示 FCM 已接受请求，不表示手机已收到、展示或用户已阅读。失效目标会记录为 `FCM_TARGET_UNREGISTERED`，上游应删除对应 FID/Token；在当前统一策略下，该结果仍会重试到 Worker 的 `max_attempts`。

主要接口：

| 方法 | 路径 | 说明 | 认证 |
|---|---|---|---|
| `GET` | `/healthz` | 进程存活检查 | 否 |
| `GET` | `/readyz` | 依赖就绪检查 | 否 |
| `GET` | `/v1/providers` | 查询当前启用的 Provider、Action 及中文功能描述 | Bearer Token |
| `POST` | `/v1/messages` | 幂等受理一条消息 | Bearer Token |
| `GET` | `/v1/messages/:source_request_id?source_system=...` | 按来源系统和业务请求 ID 查询消息状态 | Bearer Token |
| `GET` | `/docs` | Swagger UI，需显式启用 | 否 |

## MCP 使用

MCP 使用官方 Go SDK 的 Streamable HTTP 传输，默认端点为 `http://127.0.0.1:8877/mcp`，采用无服务端会话的 JSON 响应模式。客户端需要在每次请求中携带 `Authorization: Bearer <token>`。当前暴露三个工具：

| 工具 | 说明 |
|---|---|
| `submit_notification` | 幂等受理一条通知；成功只表示已经持久化受理，不表示供应商已投递成功 |
| `get_notification_status` | 按 `source_system + source_request_id` 查询持久化状态 |
| `list_provider_capabilities` | 查询运行时实际启用的 Provider 和 Action |

常见 MCP 客户端可使用下面的 HTTP Server 配置；不同客户端的外层字段名可能不同：

```json
{
  "mcpServers": {
    "notification-delivery": {
      "type": "http",
      "url": "http://127.0.0.1:8877/mcp",
      "headers": {
        "Authorization": "Bearer dev-system-token"
      }
    }
  }
}
```

端点启用了同源保护和请求体大小限制。MCP 工具直接调用共享应用服务，不会绕行调用 REST 接口。

## gRPC 使用

原生 gRPC 默认监听 `127.0.0.1:8878`，契约位于 [api/proto/notification/v1/notification.proto](api/proto/notification/v1/notification.proto)。业务 RPC 与 REST/MCP 使用相同 Bearer Token，在 Metadata 中传递 `authorization: Bearer <token>`；标准 gRPC Health 接口不要求认证。

本地配置默认启用 Reflection，可直接使用 `grpcurl`：

```bash
grpcurl -plaintext \
  -H 'authorization: Bearer dev-system-token' \
  -d '{}' \
  127.0.0.1:8878 \
  notification.v1.NotificationService/ListProviderCapabilities
```

生产环境建议设置 `GRPC_REFLECTION_ENABLED=false`。三个业务 RPC 分别为 `SubmitNotification`、`GetNotificationStatus` 和 `ListProviderCapabilities`；`payload_json` 使用 UTF-8 JSON 字节，仍由选中的 Provider Adapter 校验。

### Provider 能力发现

智能体或 MCP 工具可以通过 `GET /v1/providers` 获取当前服务真实启用的 Provider 能力。返回内容直接来自启动时构建完成的 Provider Registry，并按 `provider_code` 和 `provider_action` 稳定排序：

```json
{
  "status": 0,
  "data": {
    "providers": [
      {
        "provider_code": "lark-bot",
        "actions": [
          {
            "provider_action": "send",
            "description": "向机器人所在群会话发送文本、富文本、图片、群名片或消息卡片；非幂等"
          }
        ]
      }
    ]
  },
  "error_message": ""
}
```

调用示例：

```bash
curl http://127.0.0.1:8877/v1/providers \
  -H 'Authorization: Bearer dev-system-token'
```

### 统一响应格式

除 Swagger 静态资源与 `/docs` 跳转外，所有 JSON 接口都使用相同包络。成功时 `status` 固定为数字 `0`：

```json
{
  "status": 0,
  "data": {},
  "error_message": ""
}
```

失败时保留正确的 HTTP `4xx/5xx` 状态码，同时在响应体中返回数字业务错误码；`data` 为 `{}`，`error_message` 是可直接展示的原因：

```json
{
  "status": 1002,
  "data": {},
  "error_message": "身份认证失败，请检查 Bearer Token。"
}
```

服务根据浏览器或客户端的 `Accept-Language` 自动选择中文或英文；支持 `zh`、`zh-CN`、`en`、`en-US` 及带权重的语言列表，未提供或无法识别时默认英文。

| 数字错误码 | 含义 |
|---:|---|
| `1001` | 请求体或参数无效 |
| `1002` | 身份认证失败 |
| `1004` | 不支持供应商或动作 |
| `1005` | Payload 校验失败 |
| `1006` | 幂等请求内容冲突 |
| `1007` | 消息不存在 |
| `1008` | API 路径不存在 |
| `1009` | HTTP 方法不支持 |
| `2001` | 存储服务不可用 |
| `2002` | 必要依赖不可用 |
| `2003` | 服务内部错误 |

### Swagger 默认值

Swagger UI 会自动为 `BearerAuth` 填入实际生效的第一个 Token，并在发送请求时补上 `Bearer` 前缀。默认请求体为：

```json
{
  "payload": {
    "msg_type": "text",
    "content": {
      "text": "Notification Delivery test"
    }
  },
  "provider_action": "send",
  "provider_code": "lark-bot",
  "source_request_id": "lark-bot-send-request-id-uuid4",
  "source_system": "example-system"
}
```

`lark-bot/send` 的 Swagger 默认请求体已经是可直接校验通过的官方文本消息示例，不再使用会触发 `status=1005` 的空 `payload`。

## 配置

| 文件 | 用途 |
|---|---|
| `config/server.yaml` | REST/MCP/gRPC、数据库、MQ、认证和内嵌 Worker 配置 |
| `config/worker.yaml` | 独立 Worker、数据库、MQ 和补偿扫描器配置 |
| `config/providers.yaml` | 供应商及动作的适配器专属配置 |
| `config/providers.p0.example.yaml` | SMTP 邮件、固定端点 Webhook 和 Firebase 手机推送的完整配置示例 |

配置加载器支持 `${NAME}` 和 `${NAME:-default}` 两种环境变量表达式。常用变量包括：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DATABASE_URL` | `postgres://test:test@127.0.0.1:5432/test?sslmode=disable` | PostgreSQL DSN |
| `DB_DRIVER` | `postgres` | 数据库驱动；当前可用实现以 PostgreSQL 为准 |
| `POSTGRES_DB` | `test` | Compose 初始化的 PostgreSQL 数据库，也是 Adminer 默认登录数据库 |
| `POSTGRES_USER` | `test` | Compose PostgreSQL 用户，也是 Adminer 默认登录用户名 |
| `POSTGRES_PASSWORD` | `test` | Compose PostgreSQL 密码，也是 Adminer 默认登录密码 |
| `ADMINER_PORT` | `8081` | Adminer 本机访问端口 |
| `MQ_DRIVER` | `rabbitmq` | `rabbitmq`、`nsq` 或 `memory` |
| `RABBITMQ_URL` | `amqp://test:test@127.0.0.1:5672/` | RabbitMQ 连接地址 |
| `SWAGGER_ENABLED` | `false` | 是否启用 `/docs` |
| `MCP_ENABLED` | `true` | 是否在 HTTP 监听器上启用 MCP |
| `MCP_PATH` | `/mcp` | MCP Streamable HTTP 的精确路径 |
| `GRPC_ENABLED` | `true` | 是否启用原生 gRPC 监听器 |
| `GRPC_ADDR` | `:8878` | gRPC 监听地址 |
| `GRPC_REFLECTION_ENABLED` | `true` | 是否启用 gRPC Reflection；生产环境建议关闭 |
| `LOG_LEVEL` | `info` | 日志级别：`debug`、`info`、`warn` 或 `error` |
| `AUTH_TOKEN` | 空 | REST、MCP、gRPC 共用的 Bearer Token；为空时启动自动生成并记录到日志 |
| `LARK_BOT_WEBHOOK_URL` | 无，必填 | 飞书自定义机器人完整 Webhook |
| `LARK_BOT_SIGNING_SECRET_REF` | 空 | 可选的飞书签名密钥引用；机器人启用签名校验时配置 |
| `FIREBASE_PROJECT_ID` | 示例占位值 | Firebase 项目 ID；启用 `firebase-push/send` 时必须替换 |
| `NOTIF_CRED_NOTIFICATION_FIREBASE_SERVICE_ACCOUNT_JSON` | 空 | 示例 `credentials_ref` 对应的完整 Service Account JSON；使用 ADC 时不需要 |

`auth.tokens` 是字符串列表，可同时配置多个有效 Token；空字符串和重复值会被忽略。生产环境应通过密钥管理系统或挂载的受控配置注入数据库凭据、Broker 凭据、内部调用 Token 和供应商 Webhook，避免依赖每次启动时生成的新 Token。

内存模式排查投递链路时设置 `LOG_LEVEL=debug`。日志会按顺序记录 `notification event persisted`、`memory mq event published`、`memory mq event dequeued`、`worker event claimed`、`provider request starting` 和 `provider request completed`；只包含事件 ID、来源、Provider、队列深度和状态，不记录 Payload、Webhook、签名密钥或 Token。幂等重复请求会记录 `idempotent duplicate returned without republish`。

只有供应商明确返回成功才 ACK。所有未成功结果仍会 requeue；普通失败按数据库 `attempt_count`（真实供应商调用次数）计算 `min(default_requeue_delay × attempt_count, max_requeue_delay)`，`max_attempts=0` 表示不限制。Broker 自身的 attempts 只用于传输观测，不参与终态判断。达到真实调用上限时数据库进入 `FAILED`，不会再 requeue。若 Worker 在最后一次调用期间崩溃，恢复投递的 Claim 会暂时把计数加到上限之外；Processor 会在再次调用供应商前直接写入 `FAILED/MAX_PROVIDER_ATTEMPTS_EXHAUSTED`，并原子回退这次未发生调用的计数增量。

`lark-bot/send` 示例启用了进程内 Action 级熔断：连续 5 次可用性失败后开放 30 秒，到期只允许一个半开探测。传输错误、HTTP `408/429/5xx`、飞书 `code=11232` 和不符合协议的 HTTP 200 响应计入熔断；普通 `4xx`、其他飞书业务错误及 Adapter 内部错误不计入。熔断拒绝发生在限流器和 `REQUESTING` 之前，不调用供应商，并原子回退领取时增加的 `attempt_count`。Memory、RabbitMQ 按熔断剩余时间精确延期；NSQ 使用不触发 Consumer 全局退避的延期 requeue。进程重启后熔断状态恢复为 `CLOSED`，多个副本各自维护状态。

启用 P0 适配器前，复制 [config/providers.p0.example.yaml](config/providers.p0.example.yaml) 中需要的 Provider 到实际配置，并注入相应凭据。例如示例引用会解析为 `NOTIF_CRED_NOTIFICATION_SMTP_EMAIL_PASSWORD`、`NOTIF_CRED_NOTIFICATION_WEBHOOK_SECRET` 和 `NOTIF_CRED_NOTIFICATION_FIREBASE_SERVICE_ACCOUNT_JSON`。不要把真实 SMTP 密码、Bearer Token、HMAC Secret 或 Service Account JSON 写入 YAML。

这一策略提供更多自动恢复机会，但外部接口不支持幂等时存在重复发送风险：例如飞书已经收到消息、Worker 却在读取响应时超时，下一次 requeue 会再次发送。

飞书自定义机器人官方限制为单租户单机器人 5 次/秒、100 次/分钟；`requests_per_second` 超过 5 时服务会拒绝启动，飞书返回 `code=11232` 时会被识别为限流失败。启用签名校验的本地环境示例：

```bash
export LARK_BOT_SIGNING_SECRET_REF='vault://notification/lark-bot-signing-secret'
export NOTIF_CRED_NOTIFICATION_LARK_BOT_SIGNING_SECRET='<real-signing-secret>'
```

## 开发

运行全部测试：

```bash
go test ./...
```

执行格式化和静态检查：

```bash
go fmt ./...
go vet ./...
```

构建两个可执行文件：

```bash
go build ./cmd/server
go build ./cmd/worker
```

接口注解变化后重新生成 OpenAPI 文档：

```bash
go run github.com/swaggo/swag/cmd/swag@v1.16.4 init \
  -g main.go \
  -d cmd/server,internal/api \
  -o docs \
  --parseInternal
```

修改 gRPC 契约后重新生成 Go 代码：

```bash
protoc -I api/proto \
  --go_out=. --go_opt=module=notification-delivery \
  --go-grpc_out=. --go-grpc_opt=module=notification-delivery \
  api/proto/notification/v1/notification.proto
```

## 项目结构

```text
api/proto/              gRPC Protocol Buffers 契约
cmd/                    多协议 Server 与 Worker 程序入口
config/                 服务、Worker 和供应商示例配置
dev/                    本地 Docker Compose 环境
docs/                   生成的 OpenAPI 文档
gen/                    生成的 gRPC/Protobuf Go 代码
internal/api/           REST 路由和协议映射
internal/application/   三种协议共用的通知应用服务
internal/authn/         协议无关的静态 Bearer Token 校验
internal/grpcapi/       gRPC Server、拦截器和状态映射
internal/mcpapi/        MCP Streamable HTTP 与工具定义
internal/config/        配置加载与类型定义
internal/mq/            RabbitMQ、NSQ 和内存队列实现
internal/provider/      供应商适配器接口、注册表与成功响应判断
internal/publish/       event_id 发布与入队标记
internal/store/         数据库连接、迁移和原子状态转换
internal/worker/        消费、限流、租约和补偿扫描
```

## 当前限制

- 每条消息只能投递给一个供应商并执行一个动作。
- 当前内置 `lark-bot/send`、`smtp-email/send`、`webhook/deliver` 和 `firebase-push/send`；配置文件中出现的 Provider 才会实际启用。
- SMTP 成功只表示服务器接受邮件，Webhook 成功只表示固定端点返回 `2xx`，Firebase Push 成功只表示 FCM 返回 message ID；当前没有邮件送达、退信、手机展示/阅读或下游业务最终完成的异步回执状态。
- 除明确成功外的所有供应商发送结果都返回 MQ requeue；达到真实供应商调用 `max_attempts` 后写入 `FAILED`，没有独立 DLQ。
- 熔断器按 `provider_code/provider_action` 在进程内维护，不跨副本共享或持久化；熔断延期不消耗真实供应商尝试次数。
- 只保存最近一次投递结果，不保存完整尝试历史。
- 不承诺供应商侧“恰好一次”；无法确认外部结果时仍会重试，因此可能重复发送。
- 进程内队列不持久化，也不能独立扩容 Worker。

## 贡献

欢迎通过 Issue 讨论缺陷、适配器需求和设计变更。提交代码前请：

1. 保持“单消息、单供应商、单动作”的项目边界。
2. 为行为变化补充测试，并运行 `go test ./...` 与 `go vet ./...`。
3. 对新增供应商使用独立、严格的 Config Schema，不把供应商语义下放为任意 YAML 配置。
4. 同步更新 README、设计文档和 OpenAPI 文档中受影响的内容。

## 安全

请勿在 Issue、日志、示例 Payload 或提交记录中包含真实 Token、Webhook、证书或用户敏感数据。生产部署前应替换开发凭据、关闭 Swagger，并审查供应商响应的脱敏与截断策略。

## 许可证

仓库当前尚未包含 `LICENSE` 文件。正式以开源项目发布前需要由维护者选择并添加许可证；在此之前，代码不应被视为已获得复制、修改或分发授权。
