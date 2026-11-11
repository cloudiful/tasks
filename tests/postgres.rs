//! Live integration tests for the durable queue store.
//!
//! The tests need the fixture database of the tasks checkout. They are
//! ignored by default and run with
//! `TASKS_TEST_DATABASE_URL=<url> cargo test --test postgres -- --include-ignored --test-threads=1`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use cloudiful_tasks::{
    AttemptStatus, EnqueueRequest, Error, Failure, ListCursor, ProgressUpdate, QueuePolicy,
    RetryPolicy, TaskId, TaskKind, TaskPauseState, TaskStatus,
};
use serde_json::json;
use sqlx::postgres::PgListener;
use time::Duration as TimeDuration;
use time::OffsetDateTime;
use time::UtcOffset;
use time::macros::datetime;
use tokio::sync::Barrier;

use common::{
    app_domain_count, attempt_count, connect, connect_to_sibling_database, open_queue,
    open_queue_with_short_lease, prepare_app_domain, reset, task_count, write_app_domain,
};

const NAMESPACE: &str = "ns";
const QUEUE: &str = "q";

/// A task of an older build, whose persisted specification holds the fixed six
/// microsecond representation of the availability.
const LEGACY_TASK_SQL: &str = include_str!("../sql/tests/idempotency_legacy_task.sql");

fn request() -> EnqueueRequest {
    EnqueueRequest::new(TaskKind::new("demo").unwrap(), json!({"a": 1}))
}

async fn running_attempts(pool: &sqlx::PgPool, namespace: &str, queue: &str) -> i64 {
    sqlx::query_file_scalar!("sql/store_queue_policy_count.sql", namespace, queue)
        .fetch_one(pool)
        .await
        .expect("the running attempts must be countable")
        .unwrap_or_default()
}

async fn isolation_transaction<'a>(
    pool: &'a sqlx::PgPool,
    level: &str,
) -> sqlx::Transaction<'a, sqlx::Postgres> {
    let mut transaction = pool.begin().await.unwrap();
    let statement = match level {
        "REPEATABLE READ" => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SERIALIZABLE" => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        other => panic!("unsupported isolation level {other}"),
    };
    sqlx::query(statement)
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("SELECT 1")
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_idempotency_key_returns_the_same_task() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    let keyed = request().with_idempotency_key("key-1").unwrap();
    let first = queue.enqueue(keyed.clone()).await.unwrap();
    assert!(first.is_created());

    let second = queue.enqueue(keyed).await.unwrap();
    assert!(!second.is_created());
    assert_eq!(first.task().id(), second.task().id());
    assert_eq!(task_count(&pool, NAMESPACE, QUEUE).await, 1);

    let conflicting = EnqueueRequest::new(TaskKind::new("demo").unwrap(), json!({"a": 2}))
        .with_idempotency_key("key-1")
        .unwrap();
    let error = queue
        .enqueue(conflicting)
        .await
        .expect_err("a different request must be a conflict");
    assert!(
        matches!(error, Error::IdempotencyConflict { .. }),
        "unexpected error: {error}"
    );
    assert_eq!(task_count(&pool, NAMESPACE, QUEUE).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_legacy_request_spec_is_still_the_same_request() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    sqlx::raw_sql(LEGACY_TASK_SQL)
        .execute(&pool)
        .await
        .expect("the task of the older build must be insertable");

    let legacy = request()
        .with_available_at(datetime!(2024-01-02 03:04:05.123456 UTC))
        .with_idempotency_key("legacy-key")
        .unwrap();

    let outcome = queue.enqueue(legacy).await.unwrap();
    assert!(
        !outcome.is_created(),
        "the task of the older build must be the existing one"
    );
    assert_eq!(
        outcome.task().id(),
        TaskId::from_uuid(uuid::Uuid::from_u128(4)),
        "the existing task must be the task of the older build"
    );
    assert_eq!(
        outcome.task().available_at(),
        datetime!(2024-01-02 03:04:05.123456 UTC),
        "the persisted availability must be decoded without a loss"
    );

    let changed = request()
        .with_available_at(datetime!(2024-01-02 03:04:05.123457 UTC))
        .with_idempotency_key("legacy-key")
        .unwrap();
    let error = queue
        .enqueue(changed)
        .await
        .expect_err("a changed request must be a conflict");
    assert!(
        matches!(error, Error::IdempotencyConflict { .. }),
        "unexpected error: {error}"
    );
    assert_eq!(task_count(&pool, NAMESPACE, QUEUE).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn concurrent_enqueues_with_the_same_key_create_one_task() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    let barrier = Arc::new(Barrier::new(2));
    let first_queue = queue.clone();
    let first_barrier = barrier.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_queue
            .enqueue(request().with_idempotency_key("key-1").unwrap())
            .await
    });
    let second_queue = queue.clone();
    let second_barrier = barrier.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_queue
            .enqueue(request().with_idempotency_key("key-1").unwrap())
            .await
    });

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first.task().id(), second.task().id());
    assert_ne!(first.is_created(), second.is_created());
    assert_eq!(task_count(&pool, NAMESPACE, QUEUE).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn namespaces_and_queues_are_isolated() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let other_namespace = open_queue(&pool, "other-ns", QUEUE).await;
    let other_queue = open_queue(&pool, NAMESPACE, "other-q").await;

    let keyed = request().with_idempotency_key("shared").unwrap();
    let task = queue.enqueue(keyed.clone()).await.unwrap();
    let elsewhere = other_namespace.enqueue(keyed).await.unwrap();

    assert!(elsewhere.is_created());
    assert_ne!(task.task().id(), elsewhere.task().id());
    assert!(
        other_namespace
            .get(task.task().id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(other_queue.get(task.task().id()).await.unwrap().is_none());
    assert!(other_queue.claim("worker").await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_concurrent_claim_gives_the_lease_to_one_worker() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let first_queue = queue.clone();
    let first_barrier = barrier.clone();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_queue.claim("worker-a").await
    });
    let second_queue = queue.clone();
    let second_barrier = barrier.clone();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_queue.claim("worker-b").await
    });

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    let claims = [first, second];
    assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);

    let claim = claims.iter().flatten().next().unwrap();
    assert_eq!(claim.task().id(), task.task().id());
    assert_eq!(claim.attempt().attempt_number(), 1);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_task_that_is_not_ready_yet_is_not_claimed() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    let later = request().with_available_at(OffsetDateTime::now_utc() + TimeDuration::hours(1));
    queue.enqueue(later).await.unwrap();

    assert!(queue.claim("worker").await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn claims_are_ordered_by_priority() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    queue.enqueue(request().with_priority(0)).await.unwrap();
    queue.enqueue(request().with_priority(10)).await.unwrap();
    queue.enqueue(request().with_priority(5)).await.unwrap();

    let first = queue.claim("worker").await.unwrap().unwrap();
    let second = queue.claim("worker").await.unwrap().unwrap();
    let third = queue.claim("worker").await.unwrap().unwrap();

    assert_eq!(first.task().priority(), 10);
    assert_eq!(second.task().priority(), 5);
    assert_eq!(third.task().priority(), 0);
    assert!(queue.claim("worker").await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_expired_lease_cannot_be_used() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(400)).await;

    let error = queue
        .complete(claim.attempt(), None)
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .renew(claim.attempt())
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .report_progress(claim.attempt(), &ProgressUpdate::new("import"))
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .fail(claim.attempt(), &Failure::permanent(None))
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn recovery_seals_the_expired_attempt_and_schedules_the_task_again() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let policy = RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50)).unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(400)).await;

    let recovered = queue.recover_expired(10).await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status(), TaskStatus::Queued);
    assert_eq!(recovered[0].attempt_count(), 1);

    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Interrupted);
    assert!(attempts[0].lease_expires_at().is_none());

    let again = queue.recover_expired(10).await.unwrap();
    assert!(again.is_empty(), "the recovery must be idempotent");

    let error = queue
        .complete(claim.attempt(), None)
        .await
        .expect_err("a late worker must not finish the task");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn recovery_fails_a_task_without_budget() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    queue.claim("worker").await.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(400)).await;

    let recovered = queue.recover_expired(10).await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status(), TaskStatus::Failed);
    assert_eq!(recovered[0].terminal_reason(), Some("attempts_exhausted"));
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_failed_task_is_scheduled_again_with_a_larger_budget() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    let failed = queue
        .fail(
            claim.attempt(),
            &Failure::permanent(Some(json!({"reason": "boom"}))),
        )
        .await
        .unwrap();
    assert_eq!(failed.status(), TaskStatus::Failed);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);

    let retried = queue.retry_failed(task.task().id(), 1).await.unwrap();
    assert_eq!(retried.status(), TaskStatus::Queued);
    assert_eq!(retried.max_attempts(), 2);
    assert_eq!(
        attempt_count(&pool, task.task().id().as_uuid()).await,
        1,
        "the attempt history must stay untouched"
    );

    let error = queue
        .retry_failed(task.task().id(), 1)
        .await
        .expect_err("only a failed task may be retried");
    assert!(
        matches!(error, Error::InvalidTaskState { .. }),
        "unexpected error: {error}"
    );
    assert!(queue.retry_failed(task.task().id(), 0).await.is_err());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_retryable_failure_schedules_the_task_again() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let policy = RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50)).unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    let failed = queue
        .fail(
            claim.attempt(),
            &Failure::retryable(Some(json!({"reason": "boom"}))),
        )
        .await
        .unwrap();
    assert_eq!(failed.status(), TaskStatus::Queued);
    assert!(failed.available_at() > OffsetDateTime::now_utc());

    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Failed);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_cancellation_of_a_queued_task_is_immediate() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    let cancelled = queue.request_cancel(task.task().id()).await.unwrap();
    assert_eq!(cancelled.status(), TaskStatus::Cancelled);
    assert_eq!(
        attempt_count(&pool, task.task().id().as_uuid()).await,
        0,
        "a cancelled task must not have an attempt"
    );

    let repeated = queue.request_cancel(task.task().id()).await.unwrap();
    assert_eq!(repeated.status(), TaskStatus::Cancelled);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_cancellation_wins_over_a_later_completion() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    let cancelling = queue.request_cancel(task.task().id()).await.unwrap();
    assert_eq!(cancelling.status(), TaskStatus::CancelRequested);

    let completed = queue
        .complete(claim.attempt(), Some(json!({"value": 1})))
        .await
        .unwrap();
    assert_eq!(completed.status(), TaskStatus::Cancelled);
    assert!(completed.result().is_none(), "the result must be discarded");

    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts[0].status(), AttemptStatus::Cancelled);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_completion_wins_over_a_later_cancellation() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    let completed = queue
        .complete(claim.attempt(), Some(json!({"value": 1})))
        .await
        .unwrap();
    assert_eq!(completed.status(), TaskStatus::Succeeded);

    let cancelling = queue.request_cancel(task.task().id()).await.unwrap();
    assert_eq!(cancelling.status(), TaskStatus::Succeeded);
    assert!(cancelling.is_terminal());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn the_progress_of_an_attempt_is_stored() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    let report = ProgressUpdate::new("import")
        .with_completed(3)
        .with_total(10);
    queue
        .report_progress(claim.attempt(), &report)
        .await
        .unwrap();

    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    let stored = attempts[0].progress().expect("the progress must be stored");
    assert_eq!(stored.stage(), "import");
    assert_eq!(stored.completed(), Some(3));
    assert_eq!(stored.total(), Some(10));

    let invalid = ProgressUpdate::new("import")
        .with_completed(11)
        .with_total(10);
    let error = queue
        .report_progress(claim.attempt(), &invalid)
        .await
        .expect_err("the bounds of a report must be checked");
    assert!(
        matches!(error, Error::Validation { .. }),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_transaction_of_another_database_is_refused() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let sibling = connect_to_sibling_database().await;

    let mut transaction = sibling.begin().await.unwrap();
    let error = queue
        .enqueue_in(&mut transaction, request())
        .await
        .expect_err("a transaction of another database must be refused");
    assert!(
        matches!(
            error,
            Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
        ),
        "unexpected error: {error}"
    );
    transaction.rollback().await.ok();

    assert_eq!(task_count(&pool, NAMESPACE, QUEUE).await, 0);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_enqueue_in_a_rolled_back_transaction_leaves_no_task() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    let mut transaction = pool.begin().await.unwrap();
    let outcome = queue.enqueue_in(&mut transaction, request()).await.unwrap();
    assert!(outcome.is_created());
    transaction.rollback().await.unwrap();

    assert_eq!(task_count(&pool, NAMESPACE, QUEUE).await, 0);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_completion_in_a_rolled_back_transaction_leaves_the_task_running() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    let mut transaction = pool.begin().await.unwrap();
    let completed = queue
        .complete_in(&mut transaction, claim.attempt(), Some(json!({"value": 1})))
        .await
        .unwrap();
    assert_eq!(completed.status(), TaskStatus::Succeeded);
    transaction.rollback().await.unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts[0].status(), AttemptStatus::Running);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_refused_completion_in_a_transaction_is_reported() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut transaction = pool.begin().await.unwrap();
    let error = queue
        .complete_in(&mut transaction, claim.attempt(), None)
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    transaction.rollback().await.ok();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn the_listing_is_bounded_and_uses_a_cursor() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    for _ in 0..5 {
        queue.enqueue(request()).await.unwrap();
    }

    let first = queue.list(None, 2).await.unwrap();
    assert_eq!(first.len(), 2);
    let cursor = first[1].cursor();
    assert_eq!(
        cursor.created_at(),
        first[1].created_at(),
        "the cursor must carry the decoded creation time"
    );
    let second = queue.list(Some(cursor), 2).await.unwrap();
    assert_eq!(second.len(), 2);
    assert_ne!(first[0].id(), second[0].id());
    assert_ne!(first[1].id(), second[0].id());
    assert_eq!(
        first[0].created_at().offset(),
        UtcOffset::UTC,
        "the creation time must be decoded in UTC"
    );
    assert!(first[0].created_at() >= first[1].created_at());
    assert!(first[1].created_at() >= second[0].created_at());

    assert!(queue.list(None, 0).await.is_err());
    assert!(queue.list(None, 101).await.is_err());
    assert!(queue.attempts(first[0].id(), 0).await.is_err());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_cursor_outside_the_supported_range_is_refused() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    for _ in 0..3 {
        queue.enqueue(request()).await.unwrap();
    }

    let listed = queue.list(None, 2).await.unwrap();
    let cursor = listed[1].cursor();

    // The same moment with another offset pages the same tasks.
    let shifted = ListCursor::new(
        cursor
            .created_at()
            .to_offset(UtcOffset::from_hms(2, 0, 0).unwrap()),
        cursor.id(),
    );
    let page = queue.list(Some(shifted), 2).await.unwrap();
    let reference = queue.list(Some(cursor), 2).await.unwrap();
    assert_eq!(
        page.iter().map(|task| task.id()).collect::<Vec<_>>(),
        reference.iter().map(|task| task.id()).collect::<Vec<_>>()
    );

    // A cursor whose UTC wall clock leaves the supported range is refused.
    let out_of_range = ListCursor::new(datetime!(9999-12-31 23:59:59 -01:00), cursor.id());
    let error = queue
        .list(Some(out_of_range), 2)
        .await
        .expect_err("the cursor must be refused");
    assert!(
        matches!(
            error,
            Error::Validation {
                field: "created_at",
                ..
            }
        ),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_enqueue_wakes_the_workers_after_the_commit() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    let mut listener = PgListener::connect_with(&pool).await.unwrap();
    listener.listen("cloudiful_tasks_ready").await.unwrap();

    let mut transaction = pool.begin().await.unwrap();
    queue.enqueue_in(&mut transaction, request()).await.unwrap();
    transaction.rollback().await.unwrap();

    let rolled_back = tokio::time::timeout(Duration::from_millis(500), listener.recv()).await;
    assert!(
        rolled_back.is_err(),
        "a rolled back enqueue must not wake the workers"
    );

    queue.enqueue(request()).await.unwrap();
    let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("the enqueue must wake the workers")
        .expect("the notification must arrive");
    assert_eq!(notification.channel(), "cloudiful_tasks_ready");
    assert!(notification.payload().is_empty());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_transaction_that_started_before_the_expiry_cannot_finalize_the_task() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    // The transaction starts while the lease is still valid, it outlives it.
    let mut transaction = pool.begin().await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let error = queue
        .complete_in(&mut transaction, claim.attempt(), None)
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    transaction.rollback().await.ok();

    // The failure of a task whose transaction starts while its lease is
    // still valid and outlives it.
    let second = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let mut transaction = pool.begin().await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let error = queue
        .fail_in(
            &mut transaction,
            claim.attempt(),
            &Failure::permanent(Some(json!({"reason": "boom"}))),
        )
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    transaction.rollback().await.ok();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
    let stored = queue.get(second.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_lock_wait_that_crosses_the_expiry_refuses_the_write() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    // Another transaction holds the lock of the attempt.
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM cloudiful_tasks.attempts WHERE id = $1 FOR UPDATE")
        .bind(claim.attempt().id().as_uuid())
        .fetch_one(&mut *blocker)
        .await
        .expect("the attempt must be lockable");

    // The completion waits for the lock and outlives the lease.
    let waiting_queue = queue.clone();
    let attempt = claim.attempt().clone();
    let mut waiting = pool.begin().await.unwrap();
    let waiter = tokio::spawn(async move {
        let result = waiting_queue
            .complete_in(&mut waiting, &attempt, None)
            .await;
        (waiting, result)
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    blocker.rollback().await.unwrap();

    let (waiting, result) = waiter.await.unwrap();
    let error = result.expect_err("a lease that expired during the wait must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    waiting.rollback().await.ok();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_lock_wait_that_crosses_the_expiry_refuses_the_renewal() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    // Another transaction holds the lock of the attempt.
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM cloudiful_tasks.attempts WHERE id = $1 FOR UPDATE")
        .bind(claim.attempt().id().as_uuid())
        .fetch_one(&mut *blocker)
        .await
        .expect("the attempt must be lockable");

    // The renewal waits for the lock and outlives the lease.
    let renewing_queue = queue.clone();
    let attempt = claim.attempt().clone();
    let renewer = tokio::spawn(async move { renewing_queue.renew(&attempt).await });
    tokio::time::sleep(Duration::from_millis(400)).await;
    blocker.rollback().await.unwrap();

    let error = renewer
        .await
        .unwrap()
        .expect_err("a lease that expired during the wait must not be renewed");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_lock_wait_that_crosses_the_expiry_refuses_the_progress_report() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    // Another transaction holds the lock of the attempt.
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM cloudiful_tasks.attempts WHERE id = $1 FOR UPDATE")
        .bind(claim.attempt().id().as_uuid())
        .fetch_one(&mut *blocker)
        .await
        .expect("the attempt must be lockable");

    // The report waits for the lock and outlives the lease.
    let reporting_queue = queue.clone();
    let attempt = claim.attempt().clone();
    let reporter = tokio::spawn(async move {
        reporting_queue
            .report_progress(&attempt, &ProgressUpdate::new("import"))
            .await
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    blocker.rollback().await.unwrap();

    let error = reporter
        .await
        .unwrap()
        .expect_err("a lease that expired during the wait must not report progress");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_manual_retry_grants_only_the_requested_attempts() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let policy =
        RetryPolicy::new(10, Duration::from_millis(10), Duration::from_millis(50)).unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();
    let spec: serde_json::Value =
        sqlx::query_scalar("SELECT request_spec FROM cloudiful_tasks.tasks WHERE id = $1")
            .bind(task.task().id().as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();

    let claim = queue.claim("worker").await.unwrap().unwrap();
    let failed = queue
        .fail(
            claim.attempt(),
            &Failure::permanent(Some(json!({"reason": "boom"}))),
        )
        .await
        .unwrap();
    assert_eq!(failed.status(), TaskStatus::Failed);
    assert_eq!(failed.attempt_count(), 1);
    assert_eq!(failed.max_attempts(), 10);

    // Two retries at the same time, only one of them transitions the task.
    let first = queue.clone();
    let second = queue.clone();
    let id = task.task().id();
    let (one, two) = tokio::join!(first.retry_failed(id, 1), second.retry_failed(id, 1));
    let outcomes = [one, two];
    let granted = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    assert_eq!(granted, 1, "only one retry may transition the task");
    let retried = outcomes
        .iter()
        .find_map(|outcome| outcome.as_ref().ok())
        .expect("one retry must succeed");
    assert_eq!(retried.status(), TaskStatus::Queued);
    assert_eq!(
        retried.max_attempts(),
        2,
        "the retry grants the requested attempt only"
    );
    assert_eq!(retried.attempt_count(), 1);

    // The request of the task and its history stay untouched.
    let stored_spec: serde_json::Value =
        sqlx::query_scalar("SELECT request_spec FROM cloudiful_tasks.tasks WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        stored_spec, spec,
        "the request of the task must stay untouched"
    );
    let attempts = queue.attempts(id, 10).await.unwrap();
    assert_eq!(attempts.len(), 1, "the attempt history must stay untouched");
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_claim_in_a_transaction_is_atomic_with_the_domain_write() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    // The rollback discards the claim and the domain write together.
    let mut transaction = pool.begin().await.unwrap();
    let claim = queue
        .claim_in(&mut transaction, "worker")
        .await
        .unwrap()
        .expect("the task must be claimed");
    write_app_domain(&mut transaction, "claimed").await;
    transaction.rollback().await.unwrap();

    assert_eq!(claim.task().id(), task.task().id());
    assert_eq!(app_domain_count(&pool).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    assert_eq!(
        attempt_count(&pool, task.task().id().as_uuid()).await,
        0,
        "a rolled back claim must leave no attempt"
    );

    // The commit makes the claim and the domain write durable together.
    let mut transaction = pool.begin().await.unwrap();
    let claim = queue
        .claim_in(&mut transaction, "worker")
        .await
        .unwrap()
        .expect("the task must be claimed");
    write_app_domain(&mut transaction, "claimed").await;
    transaction.commit().await.unwrap();

    assert_eq!(claim.task().id(), task.task().id());
    assert_eq!(app_domain_count(&pool).await, 1);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_cancellation_in_a_transaction_is_atomic_with_the_domain_write() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    // The rollback discards the cancellation and the domain write together.
    let mut transaction = pool.begin().await.unwrap();
    let cancelled = queue
        .request_cancel_in(&mut transaction, task.task().id())
        .await
        .unwrap();
    assert_eq!(cancelled.status(), TaskStatus::Cancelled);
    write_app_domain(&mut transaction, "cancelled").await;
    transaction.rollback().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    assert!(stored.cancel_requested_at().is_none());

    // The commit makes the cancellation and the domain write durable together.
    let mut transaction = pool.begin().await.unwrap();
    let cancelled = queue
        .request_cancel_in(&mut transaction, task.task().id())
        .await
        .unwrap();
    assert_eq!(cancelled.status(), TaskStatus::Cancelled);
    write_app_domain(&mut transaction, "cancelled").await;
    transaction.commit().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 1);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Cancelled);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_retry_in_a_transaction_is_atomic_with_the_domain_write() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let failed = queue
        .fail(
            claim.attempt(),
            &Failure::permanent(Some(json!({"reason": "boom"}))),
        )
        .await
        .unwrap();
    assert_eq!(failed.status(), TaskStatus::Failed);
    let budget = failed.max_attempts();

    // The rollback discards the retry and the domain write together.
    let mut transaction = pool.begin().await.unwrap();
    let retried = queue
        .retry_failed_in(&mut transaction, task.task().id(), 1)
        .await
        .unwrap();
    assert_eq!(retried.status(), TaskStatus::Queued);
    write_app_domain(&mut transaction, "retried").await;
    transaction.rollback().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Failed);
    assert_eq!(stored.max_attempts(), budget);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1, "the attempt history must stay untouched");

    // The commit makes the retry and the domain write durable together.
    let mut transaction = pool.begin().await.unwrap();
    let retried = queue
        .retry_failed_in(&mut transaction, task.task().id(), 1)
        .await
        .unwrap();
    assert_eq!(retried.status(), TaskStatus::Queued);
    write_app_domain(&mut transaction, "retried").await;
    transaction.commit().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 1);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    assert_eq!(stored.max_attempts(), budget + 1);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1, "the attempt history must stay untouched");
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_recovery_in_a_transaction_is_atomic_with_the_domain_write() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let policy = RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50)).unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(400)).await;

    // The rollback discards the recovery and the domain write together.
    let mut transaction = pool.begin().await.unwrap();
    let recovered = queue
        .recover_expired_in(&mut transaction, 10)
        .await
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status(), TaskStatus::Queued);
    write_app_domain(&mut transaction, "recovered").await;
    transaction.rollback().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts[0].status(), AttemptStatus::Running);
    assert!(
        attempts[0].lease_expires_at().is_some(),
        "the lease of the attempt must stay untouched"
    );

    // The commit makes the recovery and the domain write durable together.
    let mut transaction = pool.begin().await.unwrap();
    let recovered = queue
        .recover_expired_in(&mut transaction, 10)
        .await
        .unwrap();
    assert_eq!(recovered.len(), 1);
    write_app_domain(&mut transaction, "recovered").await;
    transaction.commit().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 1);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts[0].status(), AttemptStatus::Interrupted);
    assert!(attempts[0].lease_expires_at().is_none());

    // The late worker of the sealed attempt still loses its lease.
    let error = queue
        .complete(claim.attempt(), None)
        .await
        .expect_err("a late worker must not finish the task");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_progress_report_in_a_transaction_is_atomic_with_the_domain_write() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let report = ProgressUpdate::new("import")
        .with_completed(3)
        .with_total(10);

    // The rollback discards the report and the domain write together.
    let mut transaction = pool.begin().await.unwrap();
    queue
        .report_progress_in(&mut transaction, claim.attempt(), &report)
        .await
        .unwrap();
    write_app_domain(&mut transaction, "reported").await;
    transaction.rollback().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 0);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert!(attempts[0].progress().is_none());

    // The commit makes the report and the domain write durable together.
    let mut transaction = pool.begin().await.unwrap();
    queue
        .report_progress_in(&mut transaction, claim.attempt(), &report)
        .await
        .unwrap();
    write_app_domain(&mut transaction, "reported").await;
    transaction.commit().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 1);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    let stored = attempts[0].progress().expect("the progress must be stored");
    assert_eq!(stored.stage(), "import");
    assert_eq!(stored.completed(), Some(3));
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_renewal_in_a_transaction_is_atomic_with_the_domain_write() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let lease = claim.attempt().lease_expires_at().unwrap();

    // The rollback discards the renewal and the domain write together.
    let mut transaction = pool.begin().await.unwrap();
    queue
        .renew_in(&mut transaction, claim.attempt())
        .await
        .unwrap();
    write_app_domain(&mut transaction, "renewed").await;
    transaction.rollback().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 0);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts[0].lease_expires_at(), Some(lease));

    // The commit makes the renewal and the domain write durable together.
    let mut transaction = pool.begin().await.unwrap();
    let expiry = queue
        .renew_in(&mut transaction, claim.attempt())
        .await
        .unwrap();
    write_app_domain(&mut transaction, "renewed").await;
    transaction.commit().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 1);
    assert!(expiry >= lease);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(
        attempts[0].lease_expires_at(),
        Some(expiry),
        "the renewed lease must be decoded without a loss"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_borrowed_operation_of_another_schema_instance_is_refused() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    // The schema is migrated again, the queue of the caller holds the
    // instance of the schema before the migration.
    reset(&pool).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    let mut transaction = pool.begin().await.unwrap();
    write_app_domain(&mut transaction, "refused").await;
    let error = queue
        .claim_in(&mut transaction, "worker")
        .await
        .expect_err("a transaction of another schema instance must be refused");
    assert!(
        matches!(error, Error::InstanceMismatch { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .request_cancel_in(&mut transaction, task.task().id())
        .await
        .expect_err("a transaction of another schema instance must be refused");
    assert!(
        matches!(error, Error::InstanceMismatch { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .retry_failed_in(&mut transaction, task.task().id(), 1)
        .await
        .expect_err("a transaction of another schema instance must be refused");
    assert!(
        matches!(error, Error::InstanceMismatch { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .recover_expired_in(&mut transaction, 10)
        .await
        .expect_err("a transaction of another schema instance must be refused");
    assert!(
        matches!(error, Error::InstanceMismatch { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .renew_in(&mut transaction, claim.attempt())
        .await
        .expect_err("a transaction of another schema instance must be refused");
    assert!(
        matches!(error, Error::InstanceMismatch { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .report_progress_in(
            &mut transaction,
            claim.attempt(),
            &ProgressUpdate::new("import"),
        )
        .await
        .expect_err("a transaction of another schema instance must be refused");
    assert!(
        matches!(error, Error::InstanceMismatch { .. }),
        "unexpected error: {error}"
    );
    transaction.commit().await.unwrap();

    assert_eq!(
        app_domain_count(&pool).await,
        1,
        "the refused operations must not write the domain state"
    );
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_borrowed_operation_of_another_database_is_refused() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let sibling = connect_to_sibling_database().await;

    let mut transaction = sibling.begin().await.unwrap();
    let error = queue
        .claim_in(&mut transaction, "worker")
        .await
        .expect_err("a transaction of another database must be refused");
    assert!(
        matches!(
            error,
            Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
        ),
        "unexpected error: {error}"
    );
    let error = queue
        .request_cancel_in(&mut transaction, task.task().id())
        .await
        .expect_err("a transaction of another database must be refused");
    assert!(
        matches!(
            error,
            Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
        ),
        "unexpected error: {error}"
    );
    let error = queue
        .retry_failed_in(&mut transaction, task.task().id(), 1)
        .await
        .expect_err("a transaction of another database must be refused");
    assert!(
        matches!(
            error,
            Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
        ),
        "unexpected error: {error}"
    );
    let error = queue
        .recover_expired_in(&mut transaction, 10)
        .await
        .expect_err("a transaction of another database must be refused");
    assert!(
        matches!(
            error,
            Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
        ),
        "unexpected error: {error}"
    );
    let error = queue
        .renew_in(&mut transaction, claim.attempt())
        .await
        .expect_err("a transaction of another database must be refused");
    assert!(
        matches!(
            error,
            Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
        ),
        "unexpected error: {error}"
    );
    let error = queue
        .report_progress_in(
            &mut transaction,
            claim.attempt(),
            &ProgressUpdate::new("import"),
        )
        .await
        .expect_err("a transaction of another database must be refused");
    assert!(
        matches!(
            error,
            Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
        ),
        "unexpected error: {error}"
    );
    transaction.rollback().await.ok();

    assert_eq!(task_count(&pool, NAMESPACE, QUEUE).await, 1);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_expired_lease_is_refused_by_the_borrowed_operations() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut transaction = pool.begin().await.unwrap();
    write_app_domain(&mut transaction, "reported").await;
    let error = queue
        .renew_in(&mut transaction, claim.attempt())
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    let error = queue
        .report_progress_in(
            &mut transaction,
            claim.attempt(),
            &ProgressUpdate::new("import"),
        )
        .await
        .expect_err("an expired lease must be refused");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    transaction.rollback().await.ok();

    assert_eq!(app_domain_count(&pool).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert!(attempts[0].progress().is_none());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_lock_wait_that_crosses_the_expiry_refuses_the_borrowed_renewal() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();

    // Another transaction holds the lock of the attempt.
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query_file!(
        "sql/tests/transaction_lock_attempt.sql",
        claim.attempt().id().as_uuid()
    )
    .fetch_one(&mut *blocker)
    .await
    .expect("the attempt must be lockable");

    // The borrowed renewal waits for the lock and outlives the lease.
    let waiting_queue = queue.clone();
    let attempt = claim.attempt().clone();
    let mut waiting = pool.begin().await.unwrap();
    let waiter = tokio::spawn(async move {
        write_app_domain(&mut waiting, "renewed").await;
        let result = waiting_queue.renew_in(&mut waiting, &attempt).await;
        (waiting, result)
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    blocker.rollback().await.unwrap();

    let (waiting, result) = waiter.await.unwrap();
    let error = result.expect_err("a lease that expired during the wait must not be renewed");
    assert!(
        matches!(error, Error::LeaseLost { .. }),
        "unexpected error: {error}"
    );
    waiting.rollback().await.ok();

    assert_eq!(app_domain_count(&pool).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_addressed_claim_selects_the_requested_task() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    let first = queue.enqueue(request().with_priority(10)).await.unwrap();
    let second = queue.enqueue(request().with_priority(5)).await.unwrap();
    let third = queue.enqueue(request().with_priority(0)).await.unwrap();

    let claim = queue
        .claim_by_id(third.task().id(), "worker")
        .await
        .unwrap()
        .expect("the requested task must be claimed");
    assert_eq!(claim.task().id(), third.task().id());
    assert_eq!(claim.attempt().attempt_number(), 1);

    // The earlier candidates are untouched.
    for task in [&first, &second] {
        assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 0);
        let stored = queue.get(task.task().id()).await.unwrap().unwrap();
        assert_eq!(stored.status(), TaskStatus::Queued);
    }

    // The order of the queue is unchanged.
    let next = queue.claim("worker").await.unwrap().unwrap();
    assert_eq!(next.task().id(), first.task().id());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_addressed_claim_of_an_ineligible_task_is_refused() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    // A running task, it is the only ready task when it is claimed.
    let running = queue.enqueue(request()).await.unwrap();
    let claimed = queue
        .claim("worker")
        .await
        .unwrap()
        .expect("the task must be claimed");
    assert_eq!(claimed.task().id(), running.task().id());

    // The ready task that must never be claimed as a fallback.
    let eligible = queue.enqueue(request()).await.unwrap();

    let other_namespace = open_queue(&pool, "other-ns", QUEUE).await;
    let foreign = other_namespace.enqueue(request()).await.unwrap();
    let other_queue = open_queue(&pool, NAMESPACE, "other-q").await;
    let elsewhere = other_queue.enqueue(request()).await.unwrap();
    let terminal = queue.enqueue(request()).await.unwrap();
    queue.request_cancel(terminal.task().id()).await.unwrap();
    let later = queue
        .enqueue(request().with_available_at(OffsetDateTime::now_utc() + TimeDuration::hours(1)))
        .await
        .unwrap();

    assert!(
        queue
            .claim_by_id(TaskId::new(), "worker")
            .await
            .unwrap()
            .is_none(),
        "an absent task must not be claimed"
    );
    assert!(
        queue
            .claim_by_id(foreign.task().id(), "worker")
            .await
            .unwrap()
            .is_none(),
        "a task of another namespace must not be claimed"
    );
    assert!(
        queue
            .claim_by_id(elsewhere.task().id(), "worker")
            .await
            .unwrap()
            .is_none(),
        "a task of another queue must not be claimed"
    );
    assert!(
        queue
            .claim_by_id(terminal.task().id(), "worker")
            .await
            .unwrap()
            .is_none(),
        "a terminal task must not be claimed"
    );
    assert!(
        queue
            .claim_by_id(later.task().id(), "worker")
            .await
            .unwrap()
            .is_none(),
        "a task that is not ready must not be claimed"
    );
    assert!(
        queue
            .claim_by_id(running.task().id(), "worker")
            .await
            .unwrap()
            .is_none(),
        "a running task must not be claimed"
    );

    // None of the refused calls fell back to another task.
    for task in [&eligible, &foreign, &elsewhere, &terminal, &later] {
        assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 0);
    }
    let stored = queue.get(eligible.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_addressed_claim_of_a_locked_task_is_refused() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    // Another transaction holds the lock of the task.
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query_file!(
        "sql/tests/addressed_lock_task.sql",
        task.task().id().as_uuid()
    )
    .fetch_one(&mut *blocker)
    .await
    .expect("the task must be lockable");

    assert!(
        queue
            .claim_by_id(task.task().id(), "worker")
            .await
            .unwrap()
            .is_none(),
        "a locked task must not be claimed"
    );
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 0);
    blocker.rollback().await.unwrap();

    let claim = queue
        .claim_by_id(task.task().id(), "worker")
        .await
        .unwrap()
        .expect("the task must be claimed");
    assert_eq!(claim.task().id(), task.task().id());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn two_concurrent_addressed_claims_give_the_lease_to_one_worker() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let first_queue = queue.clone();
    let first_barrier = barrier.clone();
    let first_id = task.task().id();
    let first = tokio::spawn(async move {
        first_barrier.wait().await;
        first_queue.claim_by_id(first_id, "worker-a").await
    });
    let second_queue = queue.clone();
    let second_barrier = barrier.clone();
    let second_id = task.task().id();
    let second = tokio::spawn(async move {
        second_barrier.wait().await;
        second_queue.claim_by_id(second_id, "worker-b").await
    });

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    let claims = [first, second];
    assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);

    let claim = claims.iter().flatten().next().unwrap();
    assert_eq!(claim.task().id(), task.task().id());
    assert_eq!(claim.attempt().attempt_number(), 1);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_addressed_claim_and_a_generic_claim_give_the_lease_to_one_worker() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let addressed_queue = queue.clone();
    let addressed_barrier = barrier.clone();
    let addressed_id = task.task().id();
    let addressed = tokio::spawn(async move {
        addressed_barrier.wait().await;
        addressed_queue.claim_by_id(addressed_id, "worker-a").await
    });
    let generic_queue = queue.clone();
    let generic_barrier = barrier.clone();
    let generic = tokio::spawn(async move {
        generic_barrier.wait().await;
        generic_queue.claim("worker-b").await
    });

    let addressed = addressed.await.unwrap().unwrap();
    let generic = generic.await.unwrap().unwrap();
    let claims = [addressed, generic];
    assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);

    let claim = claims.iter().flatten().next().unwrap();
    assert_eq!(claim.task().id(), task.task().id());
    assert_eq!(claim.attempt().attempt_number(), 1);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_addressed_claim_in_a_transaction_is_atomic_with_the_domain_write() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    // The rollback discards the claim and the domain write together.
    let mut transaction = pool.begin().await.unwrap();
    let claim = queue
        .claim_by_id_in(&mut transaction, task.task().id(), "worker")
        .await
        .unwrap()
        .expect("the requested task must be claimed");
    write_app_domain(&mut transaction, "claimed").await;
    transaction.rollback().await.unwrap();

    assert_eq!(claim.task().id(), task.task().id());
    assert_eq!(app_domain_count(&pool).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    assert_eq!(
        attempt_count(&pool, task.task().id().as_uuid()).await,
        0,
        "a rolled back claim must leave no attempt"
    );

    // The commit makes the claim and the domain write durable together.
    let mut transaction = pool.begin().await.unwrap();
    let claim = queue
        .claim_by_id_in(&mut transaction, task.task().id(), "worker")
        .await
        .unwrap()
        .expect("the requested task must be claimed");
    write_app_domain(&mut transaction, "claimed").await;
    transaction.commit().await.unwrap();

    assert_eq!(claim.task().id(), task.task().id());
    assert_eq!(app_domain_count(&pool).await, 1);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Running);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_addressed_claim_in_a_transaction_of_another_schema_instance_is_refused() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    // The schema is migrated again, the queue of the caller holds the
    // instance of the schema before the migration.
    reset(&pool).await;
    let task = queue.enqueue(request()).await.unwrap();

    let mut transaction = pool.begin().await.unwrap();
    write_app_domain(&mut transaction, "refused").await;
    let error = queue
        .claim_by_id_in(&mut transaction, task.task().id(), "worker")
        .await
        .expect_err("a transaction of another schema instance must be refused");
    assert!(
        matches!(error, Error::InstanceMismatch { .. }),
        "unexpected error: {error}"
    );
    transaction.commit().await.unwrap();

    assert_eq!(
        app_domain_count(&pool).await,
        1,
        "the refused claim must not write the domain state"
    );
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 0);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_queue_policy_limits_every_claim_path_across_instances() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let second = open_queue(&pool, NAMESPACE, QUEUE).await;
    queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(1),
        })
        .await
        .unwrap();
    assert_eq!(
        second.policy().await.unwrap().max_concurrent_attempts,
        Some(1)
    );

    let first = queue.enqueue(request()).await.unwrap();
    let second_task = queue.enqueue(request()).await.unwrap();
    let claim = second
        .claim("worker-1")
        .await
        .unwrap()
        .expect("the first claim must be admitted");
    assert_eq!(claim.task().id(), first.task().id());

    assert!(
        queue.claim("worker-2").await.unwrap().is_none(),
        "the second claim of the queue must be refused"
    );
    assert!(
        second
            .claim_by_id(second_task.task().id(), "worker-3")
            .await
            .unwrap()
            .is_none(),
        "an addressed claim must obey the queue policy"
    );

    let mut transaction = pool.begin().await.unwrap();
    assert!(
        second
            .claim_in(&mut transaction, "worker-4")
            .await
            .unwrap()
            .is_none(),
        "a borrowed claim must obey the queue policy"
    );
    transaction.rollback().await.unwrap();

    let mut transaction = pool.begin().await.unwrap();
    assert!(
        second
            .claim_by_id_in(&mut transaction, second_task.task().id(), "worker-5")
            .await
            .unwrap()
            .is_none(),
        "a borrowed addressed claim must obey the queue policy"
    );
    transaction.rollback().await.unwrap();

    let other_queue = open_queue(&pool, NAMESPACE, "other").await;
    other_queue.enqueue(request()).await.unwrap();
    assert!(
        other_queue.claim("worker-6").await.unwrap().is_some(),
        "another queue of the namespace must stay unlimited"
    );

    queue.complete(claim.attempt(), None).await.unwrap();
    let next = second
        .claim("worker-7")
        .await
        .unwrap()
        .expect("the sealed attempt must free the capacity");
    assert_eq!(next.task().id(), second_task.task().id());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn failure_cancellation_and_recovery_free_policy_capacity() {
    let pool = connect().await;
    reset(&pool).await;

    let failure_queue = open_queue(&pool, NAMESPACE, "failure").await;
    failure_queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(1),
        })
        .await
        .unwrap();
    failure_queue.enqueue(request()).await.unwrap();
    let failed_claim = failure_queue.claim("worker-1").await.unwrap().unwrap();
    failure_queue
        .fail(&failed_claim.attempt().clone(), &Failure::permanent(None))
        .await
        .unwrap();
    failure_queue.enqueue(request()).await.unwrap();
    assert!(
        failure_queue.claim("worker-2").await.unwrap().is_some(),
        "a sealed failure must free the capacity"
    );

    let cancel_queue = open_queue(&pool, NAMESPACE, "cancel").await;
    cancel_queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(1),
        })
        .await
        .unwrap();
    let cancelled_task = cancel_queue.enqueue(request()).await.unwrap();
    let cancelled_claim = cancel_queue.claim("worker-3").await.unwrap().unwrap();
    cancel_queue
        .request_cancel(cancelled_task.task().id())
        .await
        .unwrap();
    let cancelled = cancel_queue
        .complete(cancelled_claim.attempt(), None)
        .await
        .unwrap();
    assert_eq!(cancelled.status(), TaskStatus::Cancelled);
    cancel_queue.enqueue(request()).await.unwrap();
    assert!(
        cancel_queue.claim("worker-4").await.unwrap().is_some(),
        "a sealed cancellation must free the capacity"
    );

    let recovery_queue = open_queue_with_short_lease(&pool, NAMESPACE, "recovery").await;
    recovery_queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(1),
        })
        .await
        .unwrap();
    recovery_queue.enqueue(request()).await.unwrap();
    recovery_queue.claim("worker-5").await.unwrap().unwrap();
    assert!(
        recovery_queue.claim("worker-6").await.unwrap().is_none(),
        "the expired but unsealed attempt must still count"
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    recovery_queue.recover_expired(10).await.unwrap();
    recovery_queue.enqueue(request()).await.unwrap();
    assert!(
        recovery_queue.claim("worker-7").await.unwrap().is_some(),
        "recovery must free the capacity"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_queue_policy_is_durable_and_unlimited_by_default() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    assert_eq!(queue.policy().await.unwrap(), QueuePolicy::unlimited());

    let first = queue.enqueue(request()).await.unwrap();
    let second = queue.enqueue(request()).await.unwrap();
    assert!(queue.claim("worker-1").await.unwrap().is_some());
    assert!(
        queue.claim("worker-2").await.unwrap().is_some(),
        "an absent policy must leave the queue unlimited"
    );
    assert_eq!(task_count(&pool, NAMESPACE, QUEUE).await, 2);
    assert_ne!(first.task().id(), second.task().id());

    let error = queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(0),
        })
        .await
        .expect_err("a zero limit must be rejected");
    assert!(matches!(error, Error::Validation { .. }));

    let error = queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(u32::MAX),
        })
        .await
        .expect_err("an out of range limit must be rejected");
    assert!(matches!(error, Error::Validation { .. }));

    queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(2),
        })
        .await
        .unwrap();
    drop(queue);
    let reopened = open_queue(&pool, NAMESPACE, QUEUE).await;
    assert_eq!(
        reopened.policy().await.unwrap(),
        QueuePolicy {
            max_concurrent_attempts: Some(2),
        }
    );

    reopened.set_policy(QueuePolicy::unlimited()).await.unwrap();
    assert_eq!(reopened.policy().await.unwrap(), QueuePolicy::unlimited());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_policy_protected_claim_requires_read_committed() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(1),
        })
        .await
        .unwrap();
    let task = queue.enqueue(request()).await.unwrap();

    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let error = queue
        .claim_in(&mut transaction, "worker")
        .await
        .expect_err("a repeatable read claim must be refused");
    assert!(
        matches!(
            error,
            Error::Validation {
                field: "transaction_isolation",
                ..
            }
        ),
        "unexpected error: {error}"
    );
    transaction.rollback().await.unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 0);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_unlimited_claim_requires_read_committed() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    assert_eq!(queue.policy().await.unwrap(), QueuePolicy::unlimited());
    let task = queue.enqueue(request()).await.unwrap();

    for level in ["REPEATABLE READ", "SERIALIZABLE"] {
        let mut transaction = isolation_transaction(&pool, level).await;
        let error = queue
            .claim_in(&mut transaction, "worker")
            .await
            .expect_err("an unlimited claim under a non default isolation must be refused");
        assert_claim_requires_read_committed(&error);
        transaction.rollback().await.unwrap();

        let mut transaction = isolation_transaction(&pool, level).await;
        let error = queue
            .claim_by_id_in(&mut transaction, task.task().id(), "worker")
            .await
            .expect_err(
                "an addressed unlimited claim under a non default isolation must be refused",
            );
        assert_claim_requires_read_committed(&error);
        transaction.rollback().await.unwrap();
    }

    assert_eq!(running_attempts(&pool, NAMESPACE, QUEUE).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 0);
}

fn assert_claim_requires_read_committed(error: &Error) {
    match error {
        Error::Validation { field, reason } => {
            assert_eq!(*field, "transaction_isolation");
            assert_eq!(reason.as_str(), "all task claims require READ COMMITTED");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_old_snapshot_cannot_bypass_a_policy_installed_while_it_waits() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    assert_eq!(queue.policy().await.unwrap(), QueuePolicy::unlimited());
    let first = queue.enqueue(request()).await.unwrap();
    let second = queue.enqueue(request()).await.unwrap();

    let mut borrowed = isolation_transaction(&pool, "REPEATABLE READ").await;
    let mut addressed = isolation_transaction(&pool, "REPEATABLE READ").await;

    let running = queue
        .claim("worker-1")
        .await
        .unwrap()
        .expect("the first claim must be admitted");
    assert_eq!(running.task().id(), first.task().id());

    queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(1),
        })
        .await
        .unwrap();

    let error = queue
        .claim_in(&mut borrowed, "worker-2")
        .await
        .expect_err("an old snapshot must not bypass the new policy");
    assert_claim_requires_read_committed(&error);
    borrowed.rollback().await.unwrap();
    let error = queue
        .claim_by_id_in(&mut addressed, second.task().id(), "worker-3")
        .await
        .expect_err("an old addressed snapshot must not bypass the new policy");
    assert_claim_requires_read_committed(&error);

    assert_eq!(
        running_attempts(&pool, NAMESPACE, QUEUE).await,
        1,
        "the refused claims must leave the running count unchanged"
    );
    addressed.rollback().await.unwrap();

    let stored = queue.get(second.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Queued);
    assert_eq!(attempt_count(&pool, second.task().id().as_uuid()).await, 0);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_waiting_policy_claim_uses_a_fresh_snapshot_after_the_queue_lock() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    queue
        .set_policy(QueuePolicy {
            max_concurrent_attempts: Some(1),
        })
        .await
        .unwrap();
    let first = queue.enqueue(request()).await.unwrap();
    let second = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker-1").await.unwrap().unwrap();

    let mut holder = pool.begin().await.unwrap();
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtext('cloudiful_tasks.queue'), \
         hashtext($1 || E'\\x1f' || $2))",
    )
    .bind(NAMESPACE)
    .bind(QUEUE)
    .fetch_one(&mut *holder)
    .await
    .unwrap();

    let waiting_queue = queue.clone();
    let waiting = tokio::spawn(async move { waiting_queue.claim("worker-2").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !waiting.is_finished(),
        "the claim must wait for the queue lock"
    );

    queue
        .complete(claim.attempt(), Some(json!({"ok": true})))
        .await
        .unwrap();
    holder.commit().await.unwrap();

    let next = waiting
        .await
        .unwrap()
        .unwrap()
        .expect("the waiting claim must see the capacity of the sealed attempt");
    assert_eq!(next.task().id(), second.task().id());
    assert_eq!(first.task().id(), claim.task().id());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn pause_transitions_and_claim_exclusion() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let queued = queue.enqueue(request()).await.unwrap();
    let available_at = queued.task().available_at();

    // A queued task is paused at once and is not claimable by either path.
    let paused = queue.request_pause(queued.task().id()).await.unwrap();
    assert_eq!(paused.status(), TaskStatus::Queued);
    assert_eq!(paused.pause_state(), TaskPauseState::Paused);
    assert_eq!(paused.pause_generation(), 1);
    assert_eq!(paused.available_at(), available_at);
    assert!(queue.claim("worker").await.unwrap().is_none());
    let other = queue.enqueue(request()).await.unwrap();
    let other_claim = queue.claim("worker").await.unwrap().unwrap();
    assert_eq!(other_claim.task().id(), other.task().id());
    assert_eq!(
        queue
            .complete(other_claim.attempt(), None)
            .await
            .unwrap()
            .status(),
        TaskStatus::Succeeded
    );
    assert_eq!(
        queue
            .get(queued.task().id())
            .await
            .unwrap()
            .unwrap()
            .pause_state(),
        TaskPauseState::Paused
    );
    assert!(
        queue
            .claim_by_id(queued.task().id(), "worker")
            .await
            .unwrap()
            .is_none()
    );

    // A repeated request is idempotent and keeps the generation.
    let repeated = queue.request_pause(queued.task().id()).await.unwrap();
    assert_eq!(repeated.pause_generation(), 1);

    // A resume preserves the available time and does not bump the generation.
    let resumed = queue.resume(queued.task().id()).await.unwrap();
    assert_eq!(resumed.status(), TaskStatus::Queued);
    assert_eq!(resumed.pause_state(), TaskPauseState::Unpaused);
    assert_eq!(resumed.pause_generation(), 1);
    assert_eq!(resumed.available_at(), available_at);

    let noop = queue.resume(queued.task().id()).await.unwrap();
    assert_eq!(noop.pause_generation(), 1);
    assert_eq!(noop.pause_state(), TaskPauseState::Unpaused);

    // A running task enters pausing, keeps its lease and parks at the exact
    // attempt.
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let pausing = queue.request_pause(claim.task().id()).await.unwrap();
    assert_eq!(pausing.status(), TaskStatus::Running);
    assert_eq!(pausing.pause_state(), TaskPauseState::Pausing);
    assert_eq!(pausing.pause_generation(), 2);
    queue.renew(claim.attempt()).await.unwrap();

    let parked = queue.park_paused(claim.attempt()).await.unwrap();
    assert_eq!(parked.status(), TaskStatus::Queued);
    assert_eq!(parked.pause_state(), TaskPauseState::Paused);
    assert_eq!(parked.attempt_count(), 1);
    assert_eq!(parked.paused_attempts(), 1);

    let attempts = queue.attempts(claim.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Paused);
    assert!(attempts[0].paused_at().is_some());
    assert!(attempts[0].lease_expires_at().is_none());

    // A parked task stays unclaimable until it is resumed.
    assert!(queue.claim("worker").await.unwrap().is_none());
    assert!(
        queue
            .claim_by_id(claim.task().id(), "worker")
            .await
            .unwrap()
            .is_none()
    );

    // Cancellation wins over a paused queued task and clears the actionable
    // pause. A terminal task refuses pause and resume.
    let cancelled = queue.request_cancel(claim.task().id()).await.unwrap();
    assert_eq!(cancelled.status(), TaskStatus::Cancelled);
    assert_eq!(cancelled.pause_state(), TaskPauseState::Unpaused);
    assert!(matches!(
        queue.request_pause(claim.task().id()).await,
        Err(Error::InvalidTaskState { .. })
    ));
    assert!(matches!(
        queue.resume(claim.task().id()).await,
        Err(Error::InvalidTaskState { .. })
    ));
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn park_paused_and_stale_generation_after_resume() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let available_at = queue
        .get(task.task().id())
        .await
        .unwrap()
        .unwrap()
        .available_at();

    // The pause request advances the generation the attempt captured.
    let pausing = queue.request_pause(task.task().id()).await.unwrap();
    assert_eq!(pausing.pause_generation(), 1);
    assert_eq!(claim.attempt().pause_generation(), 0);

    // A resume clears the signal while the attempt still runs. A later park of
    // that exact attempt seals it paused but leaves the task claimable, so a
    // stale pause never stops the resumed task.
    let resumed = queue.resume(task.task().id()).await.unwrap();
    assert_eq!(resumed.pause_state(), TaskPauseState::Unpaused);
    assert_eq!(resumed.pause_generation(), 1);

    let parked = queue.park_paused(claim.attempt()).await.unwrap();
    assert_eq!(parked.status(), TaskStatus::Queued);
    assert_eq!(parked.pause_state(), TaskPauseState::Unpaused);
    assert_eq!(parked.available_at(), available_at);
    assert_eq!(parked.paused_attempts(), 1);

    // The exact parked attempt is stale now: a second park loses the lease.
    assert!(matches!(
        queue.park_paused(claim.attempt()).await,
        Err(Error::LeaseLost { .. })
    ));

    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Paused);

    // An unpaused continuation is claimable again and its attempt captures the
    // retained generation.
    let next = queue.claim("worker").await.unwrap().unwrap();
    assert_eq!(next.attempt().attempt_number(), 2);
    assert_eq!(next.attempt().pause_generation(), 1);
    let paused = queue.request_pause(task.task().id()).await.unwrap();
    assert_eq!(paused.pause_generation(), 2);
    let parked = queue.park_paused(next.attempt()).await.unwrap();
    assert_eq!(parked.status(), TaskStatus::Queued);
    assert_eq!(parked.pause_state(), TaskPauseState::Paused);
    assert_eq!(parked.paused_attempts(), 2);
    assert_eq!(parked.attempt_count(), 2);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn transactional_pause_resume_are_atomic_and_refuse_foreign_instance() {
    let pool = connect().await;
    reset(&pool).await;
    prepare_app_domain(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();

    // The rollback discards the pause and the domain write together.
    let mut transaction = pool.begin().await.unwrap();
    let paused = queue
        .request_pause_in(&mut transaction, task.task().id())
        .await
        .unwrap();
    assert_eq!(paused.pause_state(), TaskPauseState::Paused);
    write_app_domain(&mut transaction, "paused").await;
    transaction.rollback().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 0);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.pause_state(), TaskPauseState::Unpaused);
    assert_eq!(stored.pause_generation(), 0);

    // The commit makes the pause and the domain write durable together.
    let mut transaction = pool.begin().await.unwrap();
    let paused = queue
        .request_pause_in(&mut transaction, task.task().id())
        .await
        .unwrap();
    assert_eq!(paused.pause_state(), TaskPauseState::Paused);
    write_app_domain(&mut transaction, "paused").await;
    transaction.commit().await.unwrap();

    assert_eq!(app_domain_count(&pool).await, 1);
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.pause_state(), TaskPauseState::Paused);

    // The rollback of a resume keeps the pause.
    let mut transaction = pool.begin().await.unwrap();
    let resumed = queue
        .resume_in(&mut transaction, task.task().id())
        .await
        .unwrap();
    assert_eq!(resumed.pause_state(), TaskPauseState::Unpaused);
    write_app_domain(&mut transaction, "resumed").await;
    transaction.rollback().await.unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.pause_state(), TaskPauseState::Paused);
    assert_eq!(app_domain_count(&pool).await, 1);

    // The commit makes the resume durable.
    let mut transaction = pool.begin().await.unwrap();
    queue
        .resume_in(&mut transaction, task.task().id())
        .await
        .unwrap();
    write_app_domain(&mut transaction, "resumed").await;
    transaction.commit().await.unwrap();
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.pause_state(), TaskPauseState::Unpaused);
    assert_eq!(app_domain_count(&pool).await, 2);

    let claim = queue
        .claim_by_id(task.task().id(), "worker")
        .await
        .unwrap()
        .unwrap();
    queue.request_pause(task.task().id()).await.unwrap();
    let mut transaction = pool.begin().await.unwrap();
    queue
        .park_paused_in(&mut transaction, claim.attempt())
        .await
        .unwrap();
    write_app_domain(&mut transaction, "parked rollback").await;
    transaction.rollback().await.unwrap();
    assert_eq!(app_domain_count(&pool).await, 2);
    assert_eq!(
        queue.get(task.task().id()).await.unwrap().unwrap().status(),
        TaskStatus::Running
    );
    assert_eq!(
        queue.attempts(task.task().id(), 10).await.unwrap()[0].status(),
        AttemptStatus::Running
    );

    let mut transaction = pool.begin().await.unwrap();
    queue
        .park_paused_in(&mut transaction, claim.attempt())
        .await
        .unwrap();
    write_app_domain(&mut transaction, "parked commit").await;
    transaction.commit().await.unwrap();
    assert_eq!(app_domain_count(&pool).await, 3);
    assert_eq!(
        queue
            .get(task.task().id())
            .await
            .unwrap()
            .unwrap()
            .pause_state(),
        TaskPauseState::Paused
    );
    assert_eq!(
        queue.attempts(task.task().id(), 10).await.unwrap()[0].status(),
        AttemptStatus::Paused
    );

    // A transaction of another database is refused before it writes.
    let sibling = connect_to_sibling_database().await;
    let mut transaction = sibling.begin().await.unwrap();
    let error = queue
        .park_paused_in(&mut transaction, claim.attempt())
        .await
        .expect_err("foreign database must be refused");
    assert!(matches!(
        error,
        Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
    ));
    transaction.rollback().await.unwrap();

    reset(&pool).await;
    let mut transaction = pool.begin().await.unwrap();
    let error = queue
        .park_paused_in(&mut transaction, claim.attempt())
        .await
        .expect_err("foreign schema instance must be refused");
    assert!(matches!(error, Error::InstanceMismatch { .. }));
    transaction.rollback().await.unwrap();

    let sibling = connect_to_sibling_database().await;
    let mut transaction = sibling.begin().await.unwrap();
    let error = queue
        .request_pause_in(&mut transaction, task.task().id())
        .await
        .expect_err("a transaction of another database must be refused");
    assert!(
        matches!(
            error,
            Error::InstanceMismatch { .. } | Error::SchemaMissing { .. }
        ),
        "unexpected error: {error}"
    );
    transaction.rollback().await.ok();
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn paused_expired_attempt_is_recovered_without_spending_budget() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();
    queue.request_pause(task.task().id()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let restarted = open_queue_with_short_lease(&pool, NAMESPACE, QUEUE).await;
    let recovered = restarted.recover_expired(10).await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status(), TaskStatus::Queued);
    assert_eq!(recovered[0].pause_state(), TaskPauseState::Paused);
    assert_eq!(recovered[0].attempt_count(), 1);
    assert_eq!(recovered[0].paused_attempts(), 1);
    let attempts = restarted.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts[0].status(), AttemptStatus::Paused);
    assert!(attempts[0].paused_at().is_some());
    assert!(matches!(
        restarted.park_paused(claim.attempt()).await,
        Err(Error::LeaseLost { .. })
    ));
    assert!(restarted.claim("worker").await.unwrap().is_none());
    restarted.resume(task.task().id()).await.unwrap();
    assert!(restarted.claim("worker").await.unwrap().is_some());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn retryable_failure_winning_pause_clears_pause_state() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let policy = RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50)).unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();
    let claim = queue.claim("worker").await.unwrap().unwrap();
    queue.request_pause(task.task().id()).await.unwrap();
    let failed = queue
        .fail(claim.attempt(), &Failure::retryable(None))
        .await
        .unwrap();
    assert_eq!(failed.status(), TaskStatus::Queued);
    assert_eq!(failed.pause_state(), TaskPauseState::Unpaused);
    assert_eq!(failed.pause_generation(), 1);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn paused_attempts_do_not_consume_retry_budget() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let policy = RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50)).unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();

    // The first attempt fails retryably and keeps the physical ordinal.
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let failed = queue
        .fail(claim.attempt(), &Failure::retryable(None))
        .await
        .unwrap();
    assert_eq!(failed.status(), TaskStatus::Queued);
    tokio::time::sleep(Duration::from_millis(20)).await;

    // The second attempt parks at a pause checkpoint. It does not consume the
    // failure budget, so the effective used attempts stay at one.
    let claim = queue.claim("worker").await.unwrap().unwrap();
    queue.request_pause(task.task().id()).await.unwrap();
    let parked = queue.park_paused(claim.attempt()).await.unwrap();
    assert_eq!(parked.attempt_count(), 2);
    assert_eq!(parked.paused_attempts(), 1);
    queue.resume(task.task().id()).await.unwrap();

    // The third attempt fails permanently.
    let claim = queue.claim("worker").await.unwrap().unwrap();
    let failed = queue
        .fail(
            claim.attempt(),
            &Failure::permanent(Some(json!({"reason": "boom"}))),
        )
        .await
        .unwrap();
    assert_eq!(failed.status(), TaskStatus::Failed);
    assert_eq!(failed.attempt_count(), 3);
    assert_eq!(failed.paused_attempts(), 1);

    // The budget is derived from the effective attempts, not the physical
    // total: two used failure attempts plus one granted attempt.
    let retried = queue.retry_failed(task.task().id(), 1).await.unwrap();
    assert_eq!(retried.max_attempts(), 3);
    assert_eq!(retried.status(), TaskStatus::Queued);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0].status(), AttemptStatus::Failed);
    assert_eq!(attempts[1].status(), AttemptStatus::Paused);
    assert_eq!(attempts[2].status(), AttemptStatus::Failed);
}
