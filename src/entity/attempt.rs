//! The attempt table.

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "attempts", schema_name = "cloudiful_tasks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: Uuid,
    pub task_id: Uuid,
    pub attempt_number: i32,
    pub worker_id: String,
    #[sea_orm(unique)]
    pub fencing_token: Uuid,
    pub status: String,
    pub lease_expires_at: Option<TimeDateTimeWithTimeZone>,
    #[sea_orm(default_expr = "Expr::cust(\"now()\")")]
    pub started_at: TimeDateTimeWithTimeZone,
    pub finished_at: Option<TimeDateTimeWithTimeZone>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<Json>,
    #[sea_orm(column_type = "JsonBinary")]
    pub progress: Option<Json>,
    pub progress_updated_at: Option<TimeDateTimeWithTimeZone>,
    #[sea_orm(default_value = 0)]
    pub pause_generation: i64,
    pub paused_at: Option<TimeDateTimeWithTimeZone>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::task::Entity",
        from = "Column::TaskId",
        to = "super::task::Column::Id"
    )]
    Task,
}

impl Related<super::task::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Task.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
