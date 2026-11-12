# cloudiful-tasks

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

[English](README.md) | [简体中文](README.zh-CN.md)

基于 PostgreSQL 的持久化任务执行库。

`cloudiful-tasks` 将任务、执行尝试和租约保存在本库拥有的一个 PostgreSQL
schema 中。本库不会自行执行任务：worker 运行时由应用显式启动。

执行语义是**至少一次**。失去租约的 worker 仍可能完成其工作，因此处理函数
必须幂等，副作用必须用执行尝试的 fencing token 做栅栏保护。

## 能力

- 数据模型：命名空间、队列、任务类型、任务与尝试状态、重试策略、进度上报和
  fencing token，
- 显式的 schema 初始化：包含归属标记、版本化的 schema 协议和迁移校验和验证，
- 持久化队列存储：带幂等键的入队、带租约的领取、续租、进度、完成、失败、取消、
  手动重试、按任务的协作式暂停与恢复以及过期租约的恢复，
- 持久化队列策略：可选地限制队列中同时运行的尝试数量，
- worker 运行时：有界并发、租约心跳、协作式取消、过期租约恢复、唤醒通知、
  处理函数拥有的终态结果以及优雅停机。

## 安装

本库未发布到 crates.io，请使用 Git 依赖并固定你验证过的提交：

```toml
[dependencies]
cloudiful-tasks = { git = "https://github.com/cloudiful/tasks", rev = "<COMMIT_SHA>" }
sea-orm = { version = "2.0.4", default-features = false, features = [
    "macros",
    "with-json",
    "with-uuid",
    "with-time",
    "sqlx-postgres",
    "runtime-tokio-rustls",
] }
tokio-util = "0.7"
```

公开 API 接收 `sea_orm::DatabaseConnection`，因此应用负责提供连接及其连接池配置。
时间戳使用 `time::OffsetDateTime`，必须能以 UTC 表示在 `time` 默认的年份范围内
（-9999 至 9999）；不支持超出该范围的 PostgreSQL 值。持久化的可用时间使用
UTC、固定六位小数及 `Z` 后缀，因此同一个请求在不同构建之间保持同一身份。

## 使用

### schema

schema 必须显式创建。打开队列或启动 worker 都不会创建或修改 schema。

```rust
use cloudiful_tasks::{check_schema, migrate};
use sea_orm::Database;

let db = Database::connect("postgresql://user@localhost/tasks").await?;
migrate(&db).await?;
check_schema(&db).await?;
```

`migrate` 是幂等的，并且按数据库串行化：创建缺失的 schema，升级受支持的旧版本。
`check_schema` 只读，分别返回 `SchemaMissing` 或 `OutdatedSchemaVersion`。
两者都拒绝其他库的 schema（`ForeignSchema`）和更新的协议版本
（`UnsupportedSchemaVersion`），不属于本库的 schema 不会被接管或覆盖。未成功完成的
迁移记录属于脏迁移：`check_schema` 返回 `DirtyMigration`，且不会被重试。

### 队列

队列负责一个命名空间和一个队列名中的任务。

```rust
use cloudiful_tasks::{EnqueueRequest, Queue, TaskKind, WorkerConfig};
use serde_json::json;

let queue = Queue::open(db.clone(), "stock", "k8s", &WorkerConfig::default()).await?;
let request = EnqueueRequest::new(TaskKind::new("sync")?, json!({"cluster": "a"})).with_idempotency_key("sync:cluster-a")?;
queue.enqueue(request).await?;

if let Some(claim) = queue.claim("worker-1").await? {
    queue.renew(claim.attempt()).await?;
    queue.complete(claim.attempt(), Some(json!({"ok": true}))).await?;
}
```

- 同一个幂等键配同一个请求返回已有任务，即使任务已经结束；同一个键配不同请求
  返回 `IdempotencyConflict`。没有键的请求总是创建新任务。
- 只有就绪的任务会被领取，先按优先级。领取持有租约并携带全新的 fencing token；
  续租、进度上报、完成和失败都需要对应的尝试、其 token 和未过期的租约，否则
  调用返回 `LeaseLost` 且不做任何修改。
- 过期的尝试被封为 interrupted，其任务在预算允许时重新排队。可重试的失败在
  剩余预算内自动重试；`retry_failed` 用于手动重试已处于 failed 终态的任务。
  先于完成提交的取消优先生效。
- `claim_by_id` 只领取被指定的任务。队列策略通过
  `queue.set_policy(QueuePolicy { max_concurrent_attempts: Some(1) })` 设置，
  对打开同一队列的所有实例生效。

### worker

在调用 `run` 之前不会启动任何工作。

```rust
use async_trait::async_trait;
use cloudiful_tasks::{CancellationToken, HandlerError, Outcome, TaskContext, TaskHandler, TaskKind, Worker, WorkerConfig};
use serde_json::json;

struct Sync;

#[async_trait]
impl TaskHandler for Sync {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        Ok(Outcome::completed(Some(json!({"synced": true}))))
    }
}

let config = WorkerConfig::default();
let queue = Queue::open(db.clone(), "stock", "k8s", &config).await?;
let worker = Worker::builder(queue, config).handler(TaskKind::new("sync")?, Sync)?.build()?;
worker.run(CancellationToken::new()).await?;
```

- 同时运行的处理函数数量受配置限制。worker 为运行中的尝试续租，租约丢失时会
  停止处理函数并阻止其写入。
- 被取消的任务会停止其处理函数；panic 会让任务永久失败，worker 继续运行。任务的
  payload 绝不会被打印。
- `Outcome::ExternallyCompleted` 表示处理函数已经提交了该尝试的终态；worker 校验
  尝试、fencing token 和封存状态之后才跳过自己的终态写入。停机时 worker 停止
  领取、通知处理函数停止并等待宽限期。

### 协作式暂停

暂停按单个任务生效，且独立于任务状态，因此排队中的任务可以在保持 queued 的
同时暂停。

```rust
let paused = queue.request_pause(task_id).await?;
if paused.pause_state() == cloudiful_tasks::TaskPauseState::Pausing {
    // 运行中的尝试会在下一个检查点停靠。
}
queue.resume(task_id).await?;
```

运行中的任务在暂停期间保留租约。处理函数观察 `TaskContext::pause_requested`、
持久化检查点并返回 `Outcome::Paused`，worker 通过 `park_paused` 封存该次尝试。
恢复会把已暂停的任务重新排队，并保留其可用时间、尝试历史和有效失败预算。暂停
一个任务绝不会停止另一个任务。

### 事务 API

每个写操作都有一个在调用方事务中运行的变体：`enqueue_in`、`claim_in`、
`renew_in`、`report_progress_in`、`complete_in`、`fail_in`、
`request_cancel_in`、`request_pause_in`、`resume_in`、`park_paused_in`、
`retry_failed_in` 和 `recover_expired_in`。

```rust
use sea_orm::TransactionTrait;

let mut transaction = db.begin().await?;
queue.enqueue_in(&mut transaction, request).await?;
write_business_row(&mut transaction).await?;
transaction.commit().await?;
```

变体从不开启、提交或回滚事务，因此变体的结果在调用方提交之前是有效的，但在
提交之前并不持久。该事务必须属于队列所在的 schema 实例，否则调用会在写入之前
被拒绝。被拒绝的操作不能被忽略：请回滚事务，否则已经失去租约的尝试所产生的
业务副作用会被提交。

## 构建

持久化通过 SeaORM 执行。普通的读写使用本库私有的 SeaORM 实体；并发关键语句、
迁移和旧版迁移记录表位于 `sql/*.sql`，以 SeaORM 语句执行。构建不需要编译期查询
元数据，可离线完成：

```sh
cargo check --locked --offline --all-targets
```

示例展示了本库的两侧：`cargo run --example basic` 和
`cargo run --example transactional`。

测试套件是源码模块内的隔离单元测试，不需要数据库，毫秒级完成：

```sh
cargo test --locked --offline
```

单元测试套件不验证 PostgreSQL 的行为：schema、SQL 语句、并发、租约、栅栏和
迁移记录表都没有自动化测试覆盖。

## 许可证

Apache-2.0，见 [LICENSE](LICENSE)。
