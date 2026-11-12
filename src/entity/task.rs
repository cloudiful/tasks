//! The task table.

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "tasks", schema_name = "cloudiful_tasks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub namespace: String,
    pub queue: String,
    pub kind: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub result: Option<Json>,
    pub status: String,
    #[sea_orm(default_value = 0)]
    pub priority: i32,
    #[sea_orm(default_expr = "Expr::cust(\"now()\")")]
    pub available_at: TimeDateTimeWithTimeZone,
    #[sea_orm(default_expr = "Expr::cust(\"now()\")")]
    pub created_at: TimeDateTimeWithTimeZone,
    #[sea_orm(default_expr = "Expr::cust(\"now()\")")]
    pub updated_at: TimeDateTimeWithTimeZone,
    pub idempotency_key: Option<String>,
    #[sea_orm(column_type = "JsonBinary")]
    pub request_spec: Json,
    #[sea_orm(default_value = 0)]
    pub attempt_count: i32,
    #[sea_orm(default_value = 1)]
    pub max_attempts: i32,
    #[sea_orm(default_value = 0)]
    pub retry_initial_delay_ns: i64,
    #[sea_orm(default_value = 0)]
    pub retry_max_delay_ns: i64,
    pub cancel_requested_at: Option<TimeDateTimeWithTimeZone>,
    pub finished_at: Option<TimeDateTimeWithTimeZone>,
    pub terminal_reason: Option<String>,
    #[sea_orm(default_value = "unpaused")]
    pub pause_state: String,
    #[sea_orm(default_value = 0)]
    pub pause_generation: i64,
    #[sea_orm(default_value = 0)]
    pub paused_attempts: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::attempt::Entity")]
    Attempts,
}

impl Related<super::attempt::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Attempts.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
