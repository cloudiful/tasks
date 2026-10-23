# cloudiful-tasks

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

[English](README.md) | [简体中文](README.zh-CN.md)

基于 PostgreSQL 的持久化任务执行库。

`cloudiful-tasks` 将任务、执行尝试和租约保存在本库拥有的一个 PostgreSQL
schema 中。本库不会自行执行任务：worker 运行时由应用显式启动。

执行语义是**至少一次**。失去租约的 worker 仍可能完成其工作，因此处理函数
必须幂等，副作用必须用执行尝试的 fencing token 做栅栏保护。

## 状态

`v0.1` 已实现：

- 数据模型：命名空间、队列、任务类型、任务与尝试状态、重试策略、进度上报和
  fencing token，
- worker 配置约定及其校验规则，
- 有界且不会溢出的指数退避，
- 显式的 schema 初始化：包含归属标记、版本化的 schema 协议和迁移校验和验证，
- 持久化队列存储：带幂等键的入队、带租约的领取、续租、进度、完成、失败、取消、
  手动重试以及过期租约的恢复，
- 持久化队列策略：可选地限制队列中同时运行的尝试数量，
- worker 运行时：有界并发、租约心跳、协作式取消、过期租约恢复、唤醒通知、
  处理函数拥有的终态结果以及优雅停机。

## 安装

本库未发布到 crates.io，请使用 Git 依赖。仓库尚无发布版本，因此依赖固定在
经过验证的提交上：

```toml
[dependencies]
cloudiful-tasks = { git = "https://github.com/cloudiful/tasks", rev = "<COMMIT_SHA>" }
sqlx = { version = "0.9", default-features = false, features = ["postgres", "runtime-tokio", "macros", "migrate", "json", "uuid", "chrono"] }
tokio-util = "0.7"
```

请将 `<COMMIT_SHA>` 替换为你验证过的提交。公开 API 接收 `sqlx::PgPool`，因此
应用需要直接依赖 `sqlx`。

## 使用

### schema

schema 必须显式创建。打开队列或启动 worker 都不会创建或修改 schema。

```rust
use cloudiful_tasks::{check_schema, migrate};
use sqlx::postgres::PgPoolOptions;

let pool = PgPoolOptions::new()
    .max_connections(5)
    .connect("postgresql://user@localhost/tasks")
    .await?;

migrate(&pool).await?;
let schema = check_schema(&pool).await?;
println!("schema 实例 {}，协议版本 {}", schema.instance_uuid(), schema.protocol_version());
```

`migrate` 是幂等的，并且按数据库串行化，因此并发调用只会应用一次迁移。
`check_schema` 只读。

两者都会拒绝尚未就绪的数据库：

| 情况 | 结果 |
| --- | --- |
| schema 不存在 | `check_schema` 返回 `SchemaMissing`，`migrate` 创建它 |
| schema 存在但没有本库的归属标记 | `ForeignSchema` |
| schema 由更新版本的库写入 | `UnsupportedSchemaVersion` |
| 本库拥有的 schema 使用较旧的受支持协议版本 | `migrate` 会升级它；`check_schema` 返回 `OutdatedSchemaVersion` |
| 已应用的迁移与迁移源不再一致 | `ChecksumMismatch` |

不属于本库的 schema 不会被接管，也不会被覆盖。

### 队列

队列负责一个命名空间和一个队列名中的任务，队列的所有操作都按此范围隔离。

```rust
use cloudiful_tasks::{EnqueueRequest, ProgressUpdate, Queue, TaskKind, WorkerConfig};
use serde_json::json;

let queue = Queue::open(pool.clone(), "stock", "k8s", &WorkerConfig::default()).await?;

let outcome = queue
    .enqueue(EnqueueRequest::new(TaskKind::new("sync")?, json!({"cluster": "a"})))
    .await?;

if let Some(claim) = queue.claim("worker-1").await? {
    queue.renew(claim.attempt()).await?;
    queue
        .report_progress(claim.attempt(), &ProgressUpdate::new("pull").with_total(10))
        .await?;
    queue.complete(claim.attempt(), Some(json!({"ok": true}))).await?;
}
```

存储的语义：

| 主题 | 行为 |
| --- | --- |
| 幂等 | 同一个键配同一个请求返回已有任务，即使任务已经结束；同一个键配不同请求返回 `IdempotencyConflict`。没有键的请求总是创建新任务。 |
| 领取 | 只有就绪的任务会被领取，先按优先级，再按最早就绪的顺序。领取持有任务的租约，并携带全新的 fencing token。 |
| 指定领取 | `claim_by_id` 只领取被指定的任务，绝不领取其他任务。该任务必须就绪且属于本队列，否则调用返回 `None` 且不领取任何任务。该调用会绕过队列的顺序，哪个任务可以执行仍由调用方决定。`claim_by_id_in` 运行在调用方的事务中。 |
| 租约 | 租约以数据库时钟为准。续租、进度上报、完成和失败都需要对应的尝试、其 token 和未过期的租约，否则调用返回 `LeaseLost` 且不做任何修改。租约在操作取得锁之后才按数据库时钟检查，因此等待锁的调用无法使用已过期的租约。 |
| 恢复 | 过期的尝试被封为 interrupted，其任务在预算允许时重新排队，否则任务失败。恢复是幂等的。 |
| 取消 | 排队中的任务立即取消，运行中的任务被标记为待取消。先于完成提交的取消优先生效：任务以 cancelled 结束，其结果被丢弃。外部副作用无法撤销。 |
| 重试 | 除非调用方标记为可重试，否则失败即为永久失败；只有调用方通过 `retry_failed` 授予额外尝试后，任务才会再次排队。新预算为已执行的尝试次数加上授予的尝试次数，未使用的预算不会再次授予，因此预算可能变小。尝试历史和任务的请求保持不变。 |

由调用方自行决定任务准入时，按标识领取该任务：

```rust
if let Some(claim) = queue.claim_by_id(task_id, "worker-1").await? {
    queue.complete(claim.attempt(), Some(json!({"ok": true}))).await?;
}
```

该领取与其他领取一样需要租约和 fencing token，哪个任务可以执行仍由调用方
决定。

### 队列策略

策略按命名空间和队列持久化，因此打开同一队列的所有实例执行同一限制：

```rust
use cloudiful_tasks::QueuePolicy;

queue
    .set_policy(QueuePolicy {
        max_concurrent_attempts: Some(1),
    })
    .await?;

let policy = queue.policy().await?;
```

| 主题 | 行为 |
| --- | --- |
| 默认 | 没有存储策略的队列不受限制，所有已有队列的行为保持不变。 |
| 限制 | `Some(n)` 只接受受支持的正整数，拒绝零或越界值。调低限制只影响之后的领取，不会取消运行中的任务。 |
| 容量 | 所有领取路径在与策略替换相同的队列级事务咨询锁下统计运行中的尝试。已过期但尚未被恢复封存的尝试仍然计数；完成、失败、取消或恢复通过封存尝试释放容量。 |
| 隔离级别 | 所有领取路径都要求 `READ COMMITTED`，无论队列是否存有策略；其他隔离级别会在读取策略前被拒绝。加锁语句与计数分开执行，因此等待中的领取在取得锁之后才计数，不能依赖在限制写入之前取得的快照。 |
| 重置 | 设置 `None` 会删除策略并恢复不受限制的行为。 |

### worker

worker 领取其队列中的任务并运行相应类型的处理函数。在调用 `run` 之前不会
启动任何工作。

```rust
use async_trait::async_trait;
use cloudiful_tasks::{
    CancellationToken, HandlerError, Outcome, ProgressUpdate, Queue, TaskContext, TaskHandler,
    TaskKind, Worker, WorkerConfig,
};
use serde_json::json;

struct Sync;

#[async_trait]
impl TaskHandler for Sync {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        context
            .progress(&ProgressUpdate::new("sync").with_total(2))
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        Ok(Outcome::completed(Some(json!({"synced": true}))))
    }
}

let config = WorkerConfig::default();
let queue = Queue::open(pool.clone(), "stock", "k8s", &config).await?;
let worker = Worker::builder(queue, config)
    .handler(TaskKind::new("sync")?, Sync)?
    .build()?;

let shutdown = CancellationToken::new();
worker.run(shutdown).await?;
```

worker 的语义：

| 主题 | 行为 |
| --- | --- |
| 启动 | 只有调用 `run` 时 worker 才开始运行。打开队列或构建 worker 都不会启动任何工作。 |
| 并发 | 同时运行的处理函数数量受配置限制。有空闲名额时会立即领取下一个就绪任务。 |
| 心跳 | worker 为运行中的尝试续租，续租的时间不会超过剩余的租约。租约丢失、续租超时或反复的数据库错误会停止处理函数并阻止其写入，即使数据库中的租约仍然有效。 |
| 取消 | 处理函数可以观察其尝试的取消令牌。被取消的任务会停止其处理函数。 |
| 恢复 | worker 封存租约过期的尝试并重新排队其任务。 |
| 唤醒 | 队列的通知只是一个提示。worker 同时也会轮询，因此丢失的通知只会延迟下一个任务。 |
| panic | 任务永久失败，worker 继续运行。任务的 payload 绝不会被打印。 |
| 外部终态 | `Outcome::ExternallyCompleted` 表示处理函数已经通过 `Queue::complete`、`Queue::fail` 或 `Queue::complete_in`/`fail_in` 提交了该尝试的终态。worker 校验捕获的尝试 id、任务、fencing token 以及 succeeded、failed 或 cancelled 封存状态，然后跳过自己的终态写入。未封存的信号会让仍持有租约的尝试失败；租约丢失时绝不会写入更新的尝试。 |
| 停机 | worker 停止领取、通知处理函数停止、等待宽限期并中止其余任务。被中止的处理函数会被丢弃，即使它们忽略其尝试的取消令牌。它们的租约仍可被恢复，任务不会以 worker 未产生的结果结束。 |

### 事务 API

每个写操作都有一个在调用方事务中运行的变体，因此业务写入和任务共享同一次
提交：

| 操作 | 变体 |
| --- | --- |
| 入队 | `enqueue_in` |
| 领取 | `claim_in` |
| 续租 | `renew_in` |
| 进度报告 | `report_progress_in` |
| 完成 | `complete_in` |
| 失败 | `fail_in` |
| 请求取消 | `request_cancel_in` |
| 手动重试 | `retry_failed_in` |
| 恢复过期租约 | `recover_expired_in` |

```rust
let mut transaction = pool.begin().await?;
queue.enqueue_in(&mut transaction, request).await?;
write_business_row(&mut transaction).await?;
transaction.commit().await?;
```

变体返回的结果与自行提交的对应操作相同，并且从不开启、提交或回滚事务。提交和
回滚由调用方负责，因此变体的结果在调用方提交之前是有效的，但在提交之前并不
持久。`renew_in` 和 `report_progress_in` 会持有该尝试的行锁直到调用方结束事务，
因此租约是在取得锁之后才按数据库的时钟校验的。

该事务必须属于队列所在的 schema 实例，否则调用会在写入之前被拒绝。被拒绝的
操作不能被忽略：请回滚事务，否则已经失去租约的尝试所产生的业务副作用会被
提交。

两个示例展示了本库的两侧：

```sh
cargo run --example basic
cargo run --example transactional
```

## 构建

本库的所有 SQL 位于 `sql/*.sql`，通过 `query_file!` 和 `query_file_as!` 加载。
`.sqlx` 中的查询元数据已提交，因此无需数据库即可构建：

```sh
SQLX_OFFLINE=true cargo check --locked --offline --all-targets
```

仅针对共享非生产测试服务器上的专用开发数据库重新生成元数据：

```sh
DATABASE_URL="$CLOUDIFUL_TASKS_TEST_SIBLING_DATABASE_URL" \
  cargo sqlx prepare -- --all-targets
```

实时测试从显式的 `CLOUDIFUL_TASKS_TEST_DATABASE_URL`（`cloudiful_tasks_test`）
和 `CLOUDIFUL_TASKS_TEST_SIBLING_DATABASE_URL`（`cloudiful_tasks_dev`）读取
测试库 URL。守卫会在任何夹具重置之前校验实际连接选项、数据库实际身份和精确的
专用数据库注释；共享项目数据库永远不会被接受。

## 许可证

Apache-2.0，见 [LICENSE](LICENSE)。
