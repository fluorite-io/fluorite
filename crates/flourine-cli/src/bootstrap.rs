//! Bootstrap command — run migrations and create an admin API key.

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use flourine_broker::{AclChecker, ApiKeyValidator, Operation, ResourceType};

const MIGRATION_SQL: &str = include_str!("../../../migrations/001_init.sql");

pub async fn run(database_url: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await
        .context("failed to connect to database")?;

    // Run migrations
    sqlx::raw_sql(MIGRATION_SQL)
        .execute(&pool)
        .await
        .context("failed to run migrations")?;
    println!("Migrations applied.");

    let validator = ApiKeyValidator::new(pool.clone());
    let checker = AclChecker::new(pool);

    let (api_key, _) = validator
        .create_key("cli-admin", "admin:cli", None)
        .await
        .context("failed to create API key")?;

    checker
        .create_acl("admin:cli", ResourceType::Cluster, "*", Operation::Admin, true)
        .await
        .context("failed to create admin ACL")?;

    println!("Admin API key created:\n");
    println!("  {api_key}\n");
    println!("Export it:");
    println!("  export FLOURINE_API_KEY={api_key}");

    Ok(())
}
