//! The idempotency rules of an enqueue.
//!
//! An idempotency key identifies a request inside one namespace and queue.
//! The same key with the same request returns the existing task, the same key
//! with a different request is a conflict. The comparison uses the
//! deterministic request specification, not the order of the keys of the
//! payload.

use serde_json::Value;
use sqlx::PgConnection;
use std::time::Duration;
use time::format_description::StaticFormatDescription;
use time::macros::format_description;
use time::{OffsetDateTime, UtcOffset};

use crate::error::{Error, Result};
use crate::model::{EnqueueOutcome, EnqueueRequest, Namespace, QueueName, RetryPolicy, TaskId};
use crate::store;

/// Canonical representation of an availability timestamp in the persisted
/// request specification.
///
/// The representation is the UTC wall clock with exactly six fractional
/// digits, `YYYY-MM-DDTHH:MM:SS.ffffffZ`. Nanoseconds below the sixth digit
/// are truncated, the offset of the timestamp is normalized to UTC.
const AVAILABLE_AT_FORMAT: StaticFormatDescription =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]Z");

/// Builds the deterministic request specification of an enqueue request.
///
/// The specification covers every immutable part of the request. Two requests
/// with the same specification are the same intent, independent of the order
/// of the keys of the payload and of the moment the request was made.
///
/// The availability of the request is canonicalized to UTC. A timestamp whose
/// UTC wall clock lies outside the supported range is refused with an error
/// instead of a specification that cannot be compared.
pub fn canonical_request_spec(request: &EnqueueRequest) -> Result<String> {
    let mut spec = serde_json::Map::new();
    spec.insert(
        "available_at".to_owned(),
        available_at_value(request.available_at())?,
    );
    spec.insert(
        "kind".to_owned(),
        Value::String(request.kind().as_str().to_owned()),
    );
    spec.insert("payload".to_owned(), canonical_value(request.payload()));
    spec.insert("priority".to_owned(), Value::from(request.priority()));
    spec.insert("retry".to_owned(), retry_value(request.retry_policy()));
    Ok(serde_json::to_string(&Value::Object(spec))
        .expect("a request specification is serializable"))
}

/// Enqueues a task and resolves its idempotency key.
///
/// The insert and the wake up notification belong to the transaction of the
/// caller, so the notification is only delivered when that transaction
/// commits.
pub(crate) async fn enqueue_task(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    request: &EnqueueRequest,
) -> Result<EnqueueOutcome> {
    let spec = canonical_request_spec(request)?;
    let spec_value = serde_json::from_str::<Value>(&spec).map_err(|error| Error::Validation {
        field: "request",
        reason: format!("the request specification is not valid json: {error}"),
    })?;
    let task_id = TaskId::new();
    let inserted = sqlx::query_file!(
        "sql/store_enqueue.sql",
        task_id.as_uuid(),
        namespace.as_str(),
        queue.as_str(),
        request.kind().as_str(),
        request.payload().clone(),
        request.priority(),
        request.available_at(),
        request.idempotency_key().map(str::to_owned),
        spec_value.clone(),
        i32::try_from(request.retry_policy().max_attempts()).unwrap_or(i32::MAX),
        duration_nanos(request.retry_policy().initial_delay()),
        duration_nanos(request.retry_policy().max_delay()),
    )
    .fetch_optional(&mut *connection)
    .await?;

    if let Some(row) = inserted {
        let task = store::task_by_id(connection, namespace, queue, TaskId::from_uuid(row.id))
            .await?
            .ok_or(Error::NotFound)?;
        store::notify_ready(connection).await?;
        return Ok(EnqueueOutcome::Created(task));
    }

    let key = request
        .idempotency_key()
        .ok_or(Error::NotFound)
        .map(str::to_owned)?;
    let existing = sqlx::query_file_as!(
        IdempotencyMatch,
        "sql/store_idempotency_match.sql",
        namespace.as_str(),
        queue.as_str(),
        key.clone(),
        spec_value,
    )
    .fetch_optional(&mut *connection)
    .await?
    .ok_or(Error::NotFound)?;

    if !existing.spec_matches.unwrap_or(false) {
        return Err(Error::IdempotencyConflict { key });
    }

    let task = store::task_by_id(connection, namespace, queue, TaskId::from_uuid(existing.id))
        .await?
        .ok_or(Error::NotFound)?;
    Ok(EnqueueOutcome::Existing(task))
}

#[derive(sqlx::FromRow)]
struct IdempotencyMatch {
    id: uuid::Uuid,
    spec_matches: Option<bool>,
}

fn available_at_value(available_at: Option<OffsetDateTime>) -> Result<Value> {
    match available_at {
        Some(at) => {
            let utc = at
                .checked_to_offset(UtcOffset::UTC)
                .ok_or_else(|| Error::Validation {
                    field: "available_at",
                    reason: "the timestamp is outside the supported range in UTC".to_owned(),
                })?;
            let value = utc
                .format(AVAILABLE_AT_FORMAT)
                .map_err(|error| Error::Validation {
                    field: "available_at",
                    reason: format!("the timestamp cannot be represented: {error}"),
                })?;
            Ok(Value::String(value))
        }
        None => Ok(Value::Null),
    }
}

fn retry_value(retry: &RetryPolicy) -> Value {
    let mut value = serde_json::Map::new();
    value.insert(
        "initial_delay_ns".to_owned(),
        Value::from(duration_nanos(retry.initial_delay())),
    );
    value.insert("max_attempts".to_owned(), Value::from(retry.max_attempts()));
    value.insert(
        "max_delay_ns".to_owned(),
        Value::from(duration_nanos(retry.max_delay())),
    );
    Value::Object(value)
}

pub(crate) fn duration_nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let mut sorted = serde_json::Map::new();
            for (key, item) in entries {
                sorted.insert(key.clone(), canonical_value(item));
            }
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TaskKind;
    use time::macros::datetime;

    fn availability_value(available_at: OffsetDateTime) -> Value {
        available_at_value(Some(available_at)).unwrap()
    }

    #[test]
    fn the_specification_ignores_the_order_of_the_payload_keys() {
        let kind = TaskKind::new("demo").unwrap();
        let first = EnqueueRequest::new(kind.clone(), serde_json::json!({"a": 1, "b": 2}));
        let second = EnqueueRequest::new(kind, serde_json::json!({"b": 2, "a": 1}));

        assert_eq!(
            canonical_request_spec(&first).unwrap(),
            canonical_request_spec(&second).unwrap()
        );
    }

    #[test]
    fn the_specification_separates_different_requests() {
        let kind = TaskKind::new("demo").unwrap();
        let base = EnqueueRequest::new(kind.clone(), serde_json::json!({"a": 1}));
        let other = EnqueueRequest::new(kind, serde_json::json!({"a": 2}));

        assert_ne!(
            canonical_request_spec(&base).unwrap(),
            canonical_request_spec(&other).unwrap()
        );
    }

    #[test]
    fn the_availability_of_the_specification_has_six_fractional_digits() {
        assert_eq!(
            availability_value(datetime!(2024-01-02 03:04:05.123456 UTC)),
            Value::String("2024-01-02T03:04:05.123456Z".to_owned())
        );
        assert_eq!(
            availability_value(datetime!(2024-01-02 03:04:05 UTC)),
            Value::String("2024-01-02T03:04:05.000000Z".to_owned())
        );
    }

    #[test]
    fn the_availability_of_the_specification_truncates_below_the_microsecond() {
        assert_eq!(
            availability_value(datetime!(2024-01-02 03:04:05.123456789 UTC)),
            Value::String("2024-01-02T03:04:05.123456Z".to_owned())
        );
        assert_eq!(
            availability_value(datetime!(2024-01-02 03:04:05.999999999 UTC)),
            Value::String("2024-01-02T03:04:05.999999Z".to_owned())
        );
    }

    #[test]
    fn the_availability_of_the_specification_is_normalized_to_utc() {
        assert_eq!(
            availability_value(datetime!(2024-01-02 03:04:05.123456 +02:00)),
            Value::String("2024-01-02T01:04:05.123456Z".to_owned())
        );
        assert_eq!(
            availability_value(datetime!(2024-01-02 03:04:05.123456 -05:30)),
            Value::String("2024-01-02T08:34:05.123456Z".to_owned())
        );
    }

    #[test]
    fn the_availability_of_the_specification_covers_the_end_of_the_supported_range() {
        assert_eq!(
            availability_value(datetime!(9999-12-31 23:59:59 UTC)),
            Value::String("9999-12-31T23:59:59.000000Z".to_owned())
        );
    }

    #[test]
    fn an_availability_outside_the_supported_range_is_refused() {
        let error = available_at_value(Some(datetime!(9999-12-31 23:59:59 -01:00)))
            .expect_err("the utc wall clock of the timestamp is out of range");
        assert!(matches!(
            error,
            Error::Validation {
                field: "available_at",
                ..
            }
        ));
    }
}
