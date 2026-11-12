//! Native SeaQuery declarations of the schema produced by the migrations.
//!
//! The immutable files in `migrations/` remain the only upgrade path and the
//! only writer of the physical schema. These declarations describe the
//! resulting tables, constraints, indexes and foreign key for schema drift
//! verification; they are never applied when a queue or a worker is opened.

use sea_orm::sea_query::{
    Check, ColumnDef, ConditionalStatement, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Index,
    IndexCreateStatement, IndexOrder, Table, TableCreateStatement,
};

/// Declares the ownership marker table.
pub fn schema_ownership(schema: &str) -> TableCreateStatement {
    let schema = schema.to_owned();
    let mut table = Table::create();
    table
        .table((schema, "schema_ownership"))
        .col(
            ColumnDef::new("singleton")
                .boolean()
                .not_null()
                .default(Expr::value(true))
                .primary_key(),
        )
        .col(ColumnDef::new("library").text().not_null())
        .col(ColumnDef::new("protocol_version").integer().not_null())
        .col(ColumnDef::new("instance_uuid").uuid().not_null())
        .col(
            ColumnDef::new("installed_at")
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::cust("now()")),
        )
        .col(
            ColumnDef::new("updated_at")
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::cust("now()")),
        )
        .check(Check::named(
            "schema_ownership_singleton_check",
            Expr::cust("singleton"),
        ))
        .check(Check::named(
            "schema_ownership_protocol_version_check",
            Expr::cust("protocol_version >= 1"),
        ))
        .index(
            Index::create()
                .name("schema_ownership_instance_uuid_key")
                .unique()
                .col("instance_uuid"),
        );
    table
}

/// Declares the task table.
pub fn tasks(schema: &str) -> TableCreateStatement {
    let schema = schema.to_owned();
    let mut table = Table::create();
    table
        .table((schema, "tasks"))
        .col(ColumnDef::new("id").uuid().not_null().primary_key())
        .col(ColumnDef::new("namespace").text().not_null())
        .col(ColumnDef::new("queue").text().not_null())
        .col(ColumnDef::new("kind").text().not_null())
        .col(ColumnDef::new("payload").json_binary().not_null())
        .col(ColumnDef::new("result").json_binary())
        .col(ColumnDef::new("status").text().not_null())
        .col(ColumnDef::new("priority").integer().not_null().default(Expr::value(0)))
        .col(
            ColumnDef::new("available_at")
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::cust("now()")),
        )
        .col(
            ColumnDef::new("created_at")
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::cust("now()")),
        )
        .col(
            ColumnDef::new("updated_at")
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::cust("now()")),
        )
        .col(ColumnDef::new("idempotency_key").text())
        .col(ColumnDef::new("request_spec").json_binary().not_null())
        .col(ColumnDef::new("attempt_count").integer().not_null().default(Expr::value(0)))
        .col(ColumnDef::new("max_attempts").integer().not_null().default(Expr::value(1)))
        .col(
            ColumnDef::new("retry_initial_delay_ns")
                .big_integer()
                .not_null()
                .default(Expr::value(0)),
        )
        .col(
            ColumnDef::new("retry_max_delay_ns")
                .big_integer()
                .not_null()
                .default(Expr::value(0)),
        )
        .col(ColumnDef::new("cancel_requested_at").timestamp_with_time_zone())
        .col(ColumnDef::new("finished_at").timestamp_with_time_zone())
        .col(ColumnDef::new("terminal_reason").text())
        .col(
            ColumnDef::new("pause_state")
                .text()
                .not_null()
                .default(Expr::value("unpaused")),
        )
        .col(
            ColumnDef::new("pause_generation")
                .big_integer()
                .not_null()
                .default(Expr::value(0)),
        )
        .col(
            ColumnDef::new("paused_attempts")
                .integer()
                .not_null()
                .default(Expr::value(0)),
        )
        .check(Check::named(
            "tasks_status_check",
            Expr::cust("status IN ('queued', 'running', 'cancel_requested', 'succeeded', 'failed', 'cancelled')"),
        ))
        .check(Check::named(
            "tasks_namespace_check",
            Expr::cust("namespace ~ '^[-A-Za-z0-9_.:]{1,128}$'"),
        ))
        .check(Check::named(
            "tasks_queue_check",
            Expr::cust("queue ~ '^[-A-Za-z0-9_.:]{1,128}$'"),
        ))
        .check(Check::named(
            "tasks_kind_check",
            Expr::cust("kind ~ '^[-A-Za-z0-9_.:]{1,128}$'"),
        ))
        .check(Check::named(
            "tasks_idempotency_key_check",
            Expr::cust("idempotency_key IS NULL OR char_length(idempotency_key) BETWEEN 1 AND 255"),
        ))
        .check(Check::named(
            "tasks_attempt_count_check",
            Expr::cust("attempt_count >= 0"),
        ))
        .check(Check::named(
            "tasks_max_attempts_check",
            Expr::cust("max_attempts >= 1"),
        ))
        .check(Check::named(
            "tasks_retry_initial_delay_ns_check",
            Expr::cust("retry_initial_delay_ns >= 0"),
        ))
        .check(Check::named(
            "tasks_retry_max_delay_ns_check",
            Expr::cust("retry_max_delay_ns >= 0"),
        ))
        .check(Check::named(
            "tasks_pause_state_check",
            Expr::cust("pause_state IN ('unpaused', 'pausing', 'paused')"),
        ))
        .check(Check::named(
            "tasks_pause_generation_check",
            Expr::cust("pause_generation >= 0"),
        ))
        .check(Check::named(
            "tasks_paused_attempts_check",
            Expr::cust("paused_attempts >= 0 AND paused_attempts <= attempt_count"),
        ));
    table
}

/// Declares the attempt table, including its task foreign key.
pub fn attempts(schema: &str) -> TableCreateStatement {
    let schema = schema.to_owned();
    let mut table = Table::create();
    table
        .table((schema.clone(), "attempts"))
        .col(ColumnDef::new("id").uuid().not_null().primary_key())
        .col(ColumnDef::new("task_id").uuid().not_null())
        .col(ColumnDef::new("attempt_number").integer().not_null())
        .col(ColumnDef::new("worker_id").text().not_null())
        .col(ColumnDef::new("fencing_token").uuid().not_null())
        .col(ColumnDef::new("status").text().not_null())
        .col(ColumnDef::new("lease_expires_at").timestamp_with_time_zone())
        .col(
            ColumnDef::new("started_at")
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::cust("now()")),
        )
        .col(ColumnDef::new("finished_at").timestamp_with_time_zone())
        .col(ColumnDef::new("error").json_binary())
        .col(ColumnDef::new("progress").json_binary())
        .col(ColumnDef::new("progress_updated_at").timestamp_with_time_zone())
        .col(
            ColumnDef::new("pause_generation")
                .big_integer()
                .not_null()
                .default(Expr::value(0)),
        )
        .col(ColumnDef::new("paused_at").timestamp_with_time_zone())
        .check(Check::named(
            "attempts_attempt_number_check",
            Expr::cust("attempt_number >= 1"),
        ))
        .check(Check::named(
            "attempts_worker_id_check",
            Expr::cust("char_length(worker_id) BETWEEN 1 AND 128"),
        ))
        .check(Check::named(
            "attempts_status_check",
            Expr::cust("status IN ('running', 'succeeded', 'failed', 'cancelled', 'interrupted', 'paused')"),
        ))
        .check(Check::named(
            "attempts_lease_check",
            Expr::cust("(status = 'running' AND lease_expires_at IS NOT NULL) OR (status <> 'running' AND lease_expires_at IS NULL)"),
        ))
        .check(Check::named(
            "attempts_pause_generation_check",
            Expr::cust("pause_generation >= 0"),
        ))
        .check(Check::named(
            "attempts_paused_at_check",
            Expr::cust("(status = 'paused') = (paused_at IS NOT NULL)"),
        ))
        .index(
            Index::create()
                .name("attempts_fencing_token_key")
                .unique()
                .col("fencing_token"),
        )
        .index(
            Index::create()
                .name("attempts_task_number_unique")
                .unique()
                .col("task_id")
                .col("attempt_number"),
        )
        .foreign_key(
            ForeignKey::create()
                .name("attempts_task_id_fkey")
                .from((schema.clone(), "attempts"), "task_id")
                .to((schema, "tasks"), "id")
                .on_delete(ForeignKeyAction::Cascade),
        );
    table
}

/// Declares the durable queue policy table.
pub fn queue_policies(schema: &str) -> TableCreateStatement {
    let schema = schema.to_owned();
    let mut table = Table::create();
    table
        .table((schema, "queue_policies"))
        .col(ColumnDef::new("namespace").text().not_null())
        .col(ColumnDef::new("queue").text().not_null())
        .col(
            ColumnDef::new("max_concurrent_attempts")
                .integer()
                .not_null(),
        )
        .col(
            ColumnDef::new("updated_at")
                .timestamp_with_time_zone()
                .not_null()
                .default(Expr::cust("now()")),
        )
        .primary_key(Index::create().col("namespace").col("queue"))
        .check(Check::named(
            "queue_policies_max_concurrent_attempts_check",
            Expr::cust("max_concurrent_attempts > 0"),
        ))
        .check(Check::named(
            "queue_policies_namespace_check",
            Expr::cust("namespace ~ '^[-A-Za-z0-9_.:]{1,128}$'"),
        ))
        .check(Check::named(
            "queue_policies_queue_check",
            Expr::cust("queue ~ '^[-A-Za-z0-9_.:]{1,128}$'"),
        ));
    table
}

/// Declares the indexes of the task and attempt tables.
pub fn indexes(schema: &str) -> Vec<IndexCreateStatement> {
    let schema = schema.to_owned();
    vec![
        Index::create()
            .name("tasks_idempotency_key_unique")
            .unique()
            .table((schema.clone(), "tasks"))
            .col("namespace")
            .col("queue")
            .col("idempotency_key")
            .and_where(Expr::col("idempotency_key").is_not_null())
            .to_owned(),
        Index::create()
            .name("tasks_claim_idx")
            .table((schema.clone(), "tasks"))
            .col("namespace")
            .col("queue")
            .col(("priority", IndexOrder::Desc))
            .col("available_at")
            .col("created_at")
            .col("id")
            .and_where(Expr::cust("status = 'queued'"))
            .to_owned(),
        Index::create()
            .name("tasks_namespace_queue_idx")
            .table((schema.clone(), "tasks"))
            .col("namespace")
            .col("queue")
            .col(("created_at", IndexOrder::Desc))
            .col(("id", IndexOrder::Desc))
            .to_owned(),
        Index::create()
            .name("attempts_one_active_per_task")
            .unique()
            .table((schema.clone(), "attempts"))
            .col("task_id")
            .and_where(Expr::cust("status = 'running'"))
            .to_owned(),
        Index::create()
            .name("attempts_lease_idx")
            .table((schema.clone(), "attempts"))
            .col("status")
            .col("lease_expires_at")
            .to_owned(),
    ]
}
