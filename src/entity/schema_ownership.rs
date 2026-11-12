//! The ownership marker of the library schema.

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "schema_ownership", schema_name = "cloudiful_tasks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub singleton: bool,
    pub library: String,
    pub protocol_version: i32,
    #[sea_orm(unique)]
    pub instance_uuid: Uuid,
    #[sea_orm(default_expr = "Expr::cust(\"now()\")")]
    pub installed_at: TimeDateTimeWithTimeZone,
    #[sea_orm(default_expr = "Expr::cust(\"now()\")")]
    pub updated_at: TimeDateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
