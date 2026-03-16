//! main.rs
//!
//! This file demonstrates how to use the global PostgreSQL connection pool
//! defined in `db_config` and how to execute database queries using
//! `deadpool-postgres` with `tokio-postgres`.
//!
//! The goal of this example is to show two common patterns when interacting
//! with a database:
//!
//! 1. Executing **simple queries without a transaction**
//! 2. Executing queries **inside a transaction**
//!
//! -----------------------------------------------------------------------------
//! IMPORTANT NOTE ABOUT SELECT QUERIES
//! -----------------------------------------------------------------------------
//!
//! In most cases, **SELECT queries do NOT need an explicit transaction**.
//!
//! PostgreSQL automatically wraps each individual statement in an implicit
//! transaction when one is not explicitly started.
//!
//! Example (implicit behavior):
//!
//! ```sql
//! BEGIN;
//! SELECT * FROM users;
//! COMMIT;
//! ```
//!
//! Because of this, simple read queries typically do **not need explicit
//! transaction management**.
//!
//! However, transactions become important when multiple queries must behave
//! atomically or when consistent reads are required.
//!
//! -----------------------------------------------------------------------------
//! TYPICAL CASES WHERE TRANSACTIONS ARE UNNECESSARY
//! -----------------------------------------------------------------------------
//!
//! These are the most common read patterns in backend services.
//!
//! -> **Simple SELECT**
//!
//!   A query that retrieves data without modifying anything.
//!
//!   Example:
//!
//!   ```sql
//!   SELECT * FROM users;
//!   ```
//!
//!
//! -> **Pagination queries**
//!
//!   Queries used to fetch data in pages for APIs or UI lists.
//!
//!   Example:
//!
//!   ```sql
//!   SELECT * FROM users ORDER BY id LIMIT 20 OFFSET 40;
//!   ```
//!
//!   This retrieves a subset of rows to display a specific page of results.
//!
//!
//! -> **Fetching a single record**
//!
//!   Very common in APIs where a specific resource is requested.
//!
//!   Example:
//!
//!   ```sql
//!   SELECT * FROM users WHERE id = 10;
//!   ```
//!
//!
//! -> **Dashboard queries**
//!
//!   Queries used for statistics, reports, or monitoring dashboards.
//!
//!   Example:
//!
//!   ```sql
//!   SELECT COUNT(*) FROM users;
//!   ```
//!
//!
//! -> **Lookups**
//!
//!   Fast queries used to retrieve reference data such as IDs, names,
//!   configuration values, etc.
//!
//!   Example:
//!
//!   ```sql
//!   SELECT id FROM roles WHERE name = 'admin';
//!   ```
//!
//!
//! These operations are **read-only and independent**, so they do not require
//! explicit transactions.
//!
//! -----------------------------------------------------------------------------
//! WHEN TRANSACTIONS ARE NECESSARY
//! -----------------------------------------------------------------------------
//!
//! Transactions are required when multiple operations must behave as a single
//! atomic unit or when consistent reads are required.
//!
//! Common cases include:
//!
//! • Multiple related updates
//! • Read → modify → write workflows
//! • Ensuring consistent reads across multiple queries
//! • Financial operations (transfers, balances, etc.)
//! • Operations that must either **fully succeed or fully fail**
//!
//! Even though transactions are usually used with write operations
//! (INSERT / UPDATE / DELETE), they can also be used with SELECT queries
//! when a consistent snapshot of the data is required.
//!
//! -----------------------------------------------------------------------------
//! NOTE ABOUT THE TRANSACTION EXAMPLE
//! -----------------------------------------------------------------------------
//!
//! In this file we intentionally use **SELECT queries inside a transaction**
//! only for demonstration purposes.
//!
//! This avoids modifying any data while still showing how to structure
//! transactional code (commit / rollback).
//!
//! In a real system, the transaction would typically contain writes or
//! multi-step logic.
//!
//! -----------------------------------------------------------------------------

mod db_config;

use dotenvy::from_filename;
use std::env;
use dirs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // -------------------------------------------------------------------------
    // LOAD ENVIRONMENT VARIABLES
    // -------------------------------------------------------------------------
    let mut path = dirs::config_dir()
        .ok_or("Cannot find configuration directory")?;

    path.push("rust-postgres-pool/.env");

    from_filename(&path)?;

    // -------------------------------------------------------------------------
    // STEP 1 — Initialize the global connection pool
    // -------------------------------------------------------------------------
    //
    // Initialize the database parameters from environment variables.
    //
    // These variables are loaded from the `.env` file located in the
    // OS-specific configuration directory earlier in the startup process.
    //
    // The resulting parameters are used to create the global connection pool,
    // which will be shared across the entire application.
    //
    // The pool maintains multiple connections and distributes them among
    // concurrent tasks. Initialization must occur only once during
    // application startup.
    let params = db_config::DbParams {
        host: env::var("DB_HOST")?,
        port: env::var("DB_PORT")?.parse()?,
        user: env::var("DB_USER")?,
        password: env::var("DB_PASSWORD")?,
        db_name: Some(env::var("DB_NAME")?),
        pool_max_size: env::var("DB_POOL_SIZE")?.parse()?,
    };

    db_config::init_global_pool(params).await?;

    // -------------------------------------------------------------------------
    // EXAMPLE 1 — NON-TRANSACTIONAL QUERY
    // -------------------------------------------------------------------------
    //
    // This is the most common pattern used in backend services.
    //
    // For simple read operations, there is no need to explicitly open a
    // transaction.
    //
    // PostgreSQL internally wraps the statement in its own transaction,
    // so the application does not need to manage commit/rollback.
    //

    let client = db_config::get_client().await?;

    let rows = client
        .query("SELECT * FROM users", &[])
        .await?;

    println!("--- Non-transactional query results ---");

    for row in rows {

        let id: i32 = row.get("id");
        let username: String = row.get("username");

        println!("user: id={id}, username={username}");
    }

    // -------------------------------------------------------------------------
    // EXAMPLE 2 — TRANSACTIONAL QUERY
    // -------------------------------------------------------------------------
    //
    // In this example we demonstrate how to structure code that runs
    // inside a transaction.
    //
    // Even though we are using SELECT statements here, the purpose is
    // purely educational so that the commit/rollback workflow is clear.
    //
    // In real-world scenarios, transactions usually contain write
    // operations or multi-step logic that must remain consistent.

    let mut client = db_config::get_client().await?;

    // Start a transaction
    let tx = client.transaction().await?;

    // Execute transactional logic inside an async block
    // so we can easily detect errors and decide whether
    // to commit or rollback.
    let result: Result<(), tokio_postgres::Error> = async {

        let rows = tx.query(
            "SELECT * FROM users",
            &[],
        ).await?;

        println!("--- Transactional query results ---");

        for row in rows {

            let id: i32 = row.get("id");
            let username: String = row.get("username");

            println!("user: id={id}, username={username}");
        }

        // If this block returns Ok(()), the transaction will commit.
        // If an error occurs, the transaction will rollback.

        Ok(())
    }
    .await;

    // -------------------------------------------------------------------------
    // STEP 3 — Commit or Rollback
    // -------------------------------------------------------------------------
    //
    // If everything succeeded, we commit the transaction.
    // Otherwise we rollback all operations performed within it.

    match result {
        Ok(()) => {
            tx.commit().await?;
            println!("COMMIT successful");
        }
        Err(e) => {
            tx.rollback().await?;
            eprintln!("ROLLBACK due to error: {e}");
        }
    }

    Ok(())
}
