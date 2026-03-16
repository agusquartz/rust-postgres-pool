//! db_config.rs
//!
//! Database pool configuration and access layer using `deadpool_postgres`.
//!
//! This module provides a **global PostgreSQL connection pool** that can be
//! accessed safely from anywhere in the application.
//!
//! The design is intentionally minimal because **deadpool_postgres already
//! handles most of the complexity internally**, such as:
//!
//! - connection pooling
//! - concurrent access
//! - connection recycling
//! - stale connection detection
//! - automatic reconnection when a connection is broken
//!
//! Therefore we only need a small wrapper to:
//!
//! - initialize the pool once
//! - expose a simple `get_client()` API
//!
//! -----------------------------------------------------------------------------
//! POOLING MODEL
//! -----------------------------------------------------------------------------
//!
//! Instead of maintaining a single database connection protected by a mutex,
//! we use a **connection pool**.
//!
//! A pool maintains multiple open database connections and hands them out
//! to callers when needed.
//!
//! Typical flow:
//!
//! ```text
//! application
//!      │
//!      ▼
//!  get_client()
//!      │
//!      ▼
//!   connection pool
//!      │
//!      ▼
//!  available connection
//! ```
//!
//! When the client goes out of scope, it is **returned automatically to the pool**.
//!
//! This allows many concurrent database operations without locking.
//!
//! -----------------------------------------------------------------------------
//! KEEPALIVE / RECYCLING
//! -----------------------------------------------------------------------------
//!
//! `deadpool_postgres` internally checks the health of connections before
//! returning them from the pool.
//!
//! The behavior depends on the `RecyclingMethod` used.
//!
//! We use:
//!
//! ```text
//! RecyclingMethod::Verified
//! ```
//!
//! This means:
//!
//! Before a connection is handed to the caller, the pool executes:
//!
//! ```sql
//! SELECT 1
//! ```
//!
//! If the query fails, the connection is considered **dead or stale** and is
//! automatically discarded and replaced with a new connection.
//!
//! Therefore we get **automatic reconnection behavior** without writing custom
//! logic like:
//!
//! - ensure_connected()
//! - health checks
//! - keepalive tasks
//! - manual reconnect logic
//!
//! -----------------------------------------------------------------------------
//! GLOBAL STATE MANAGEMENT
//! -----------------------------------------------------------------------------
//!
//! We store the pool inside a `OnceCell`.
//!
//! `OnceCell<T>` is a synchronization primitive that allows **one-time
//! initialization of a static value**.
//!
//! It guarantees that:
//!
//! - the value can only be initialized once
//! - reads are lock-free
//! - it is thread-safe
//!
//! This is perfect for resources such as:
//!
//! - database pools
//! - configuration
//! - logging infrastructure
//!
//! -----------------------------------------------------------------------------
//! WHY WE DO NOT USE Arc<Mutex<_>>
//! -----------------------------------------------------------------------------
//!
//! We intentionally avoid:
//!
//! ```rust
//! Arc<Mutex<Client>>
//! ```
//!
//! because:
//!
//! 1. `deadpool::Pool` is already **thread-safe**
//! 2. it internally uses synchronization primitives
//! 3. locking the entire client would **serialize all queries**
//!
//! That would defeat the purpose of using a connection pool.
//!
//! Instead, each call to `get_client()` retrieves **a separate connection
//! from the pool**, allowing true concurrency.
//!
//! -----------------------------------------------------------------------------
//! CLIENT LIFETIME
//! -----------------------------------------------------------------------------
//!
//! When calling:
//!
//! ```rust
//! let client = get_client().await?;
//! ```
//!
//! The pool gives us a `deadpool_postgres::Client`.
//!
//! Internally this is a wrapper around a `tokio_postgres::Client`.
//!
//! When the `client` variable is dropped, the connection is **automatically
//! returned to the pool**.
//!
//! No manual cleanup is necessary.

use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use once_cell::sync::OnceCell;
use tokio_postgres::{Config, NoTls};

/// Custom database error type.
///
/// This aggregates the main errors that can occur while interacting with
/// the database layer.
///
/// The goal is to expose a **single unified error type** to the rest of
/// the application.
#[derive(Debug, thiserror::Error)]
pub enum DbError {

    /// Returned when the pool has not been initialized yet.
    ///
    /// This happens if `get_client()` is called before `init_global_pool()`.
    #[error("database pool not initialized")]
    NotInitialized,

    /// Error returned by `deadpool`.
    ///
    /// This may happen when:
    /// - no connections are available
    /// - the pool fails to create a new connection
    /// - the pool is exhausted
    #[error("pool error: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),

    /// PostgreSQL driver error from `tokio-postgres`.
    ///
    /// This represents errors returned directly by the database server.
    #[error("postgres error: {0}")]
    Pg(#[from] tokio_postgres::Error),

    /// Returned if someone tries to initialize the pool more than once.
    ///
    /// Since the pool is stored inside a `OnceCell`, it can only be set once.
    #[error("pool already initialized")]
    AlreadyInitialized,
}

/// Configuration parameters used to initialize the connection pool.
///
/// This struct is intentionally simple and contains only the parameters
/// required to establish database connections.
#[derive(Clone, Debug)]
pub struct DbParams {

    /// Database host (example: `127.0.0.1`)
    pub host: String,

    /// PostgreSQL port (default: 5432)
    pub port: u16,

    /// Database user
    pub user: String,

    /// Database password
    pub password: String,

    /// Optional database name.
    ///
    /// If `None`, PostgreSQL will use the default database for the user.
    pub db_name: Option<String>,

    /// Maximum number of connections allowed in the pool.
    ///
    /// Typical values:
    ///
    /// - small services: 8–16
    /// - medium services: 16–32
    /// - high-throughput systems: 32–100+
    pub pool_max_size: usize,
}

/// Global connection pool.
///
/// `OnceCell` guarantees:
///
/// - safe concurrent access
/// - exactly one initialization
/// - lock-free reads
///
/// This makes it ideal for storing application-wide resources.
static DB_POOL: OnceCell<Pool> = OnceCell::new();

/// Initializes the global database pool.
///
/// This function must be called **once during application startup**.
///
/// Example:
///
/// ```rust
/// init_global_pool(params).await?;
/// ```
///
/// Internally this function:
///
/// 1. Creates a `tokio_postgres::Config`.
/// 2. Wraps it in a `deadpool_postgres::Manager`.
/// 3. Builds a `Pool`.
/// 4. Stores the pool inside the global `OnceCell`.
///
/// After initialization, the pool can be accessed anywhere using
/// `get_client()`.
pub async fn init_global_pool(params: DbParams) -> Result<(), DbError> {

    // Step 1 — build postgres configuration.
    //
    // `tokio_postgres::Config` stores connection parameters that will be
    // used whenever the pool creates a new connection.
    let mut cfg = Config::new();

    cfg.host(&params.host);
    cfg.port(params.port);
    cfg.user(&params.user);
    cfg.password(&params.password);

    if let Some(db) = params.db_name {
        cfg.dbname(&db);
    }

    // -------------------------------------------------------------------------
    // TCP KEEPALIVE CONFIGURATION
    // -------------------------------------------------------------------------
    //
    // TCP keepalive is a mechanism used by the operating system to detect
    // broken or half-open TCP connections.
    //
    // This situation happens when:
    //
    // - a network cable is unplugged
    // - a router silently drops connections
    // - a firewall kills idle connections
    // - a laptop goes to sleep
    //
    // Without TCP keepalive, a dead TCP connection may remain open for
    // **several minutes or even hours**, because neither side realizes the
    // connection has been lost.
    //
    // When keepalive is enabled, the OS periodically sends a small probe
    // packet to verify that the peer is still reachable.
    //
    // If the peer does not respond, the OS marks the socket as dead,
    // allowing the database driver and the pool to detect the failure
    // earlier.
    //
    // This works together with `deadpool_postgres` recycling:
    //
    // 1. TCP keepalive helps detect broken sockets.
    // 2. `RecyclingMethod::Verified` runs `SELECT 1` before returning
    //    a connection from the pool.
    // 3. If the connection is dead, the pool discards it and opens
    //    a new one automatically.
    //
    // This combination provides robust behavior in unstable networks.
    //
    // Typical keepalive interval values:
    //
    // - 30 seconds → aggressive detection
    // - 60 seconds → common production setting
    // - 120+ seconds → lower overhead
    //
    // Here we use 30 seconds to detect failures relatively quickly.
    cfg.keepalives(true);

    // Time before the first keepalive probe is sent when the connection
    // has been idle.
    //
    // After this period of inactivity, the operating system sends
    // a TCP keepalive packet to verify that the connection is still alive.
    //
    // If the peer does not respond, the socket will eventually be marked
    // as closed and the connection pool will recreate it automatically.
    cfg.keepalives_idle(Duration::from_secs(60));

    // Step 2 — configure connection recycling.
    //
    // `RecyclingMethod::Verified` ensures that every connection returned
    // from the pool is first validated using a lightweight query.
    //
    // This prevents the application from receiving connections that were
    // silently closed by:
    //
    // - PostgreSQL
    // - network infrastructure
    // - firewalls
    // - load balancers
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Verified,
    };

    // Step 3 — create the pool manager.
    //
    // The manager is responsible for:
    //
    // - opening new connections
    // - recycling old connections
    // - validating connections
    let manager = Manager::from_config(cfg, NoTls, mgr_config);

    // Step 4 — build the connection pool.
    //
    // The pool will lazily create connections as needed until it reaches
    // `max_size`.
    let pool = Pool::builder(manager)
        .max_size(params.pool_max_size)
        .runtime(Runtime::Tokio1)
        .build()
        .unwrap();

    // Step 5 — store the pool in the global OnceCell.
    //
    // If the pool was already initialized, this returns an error.
    DB_POOL
        .set(pool)
        .map_err(|_| DbError::AlreadyInitialized)?;

    Ok(())
}

/// Retrieves a database client from the global pool.
///
/// This function:
///
/// 1. Retrieves the global pool.
/// 2. Asks the pool for an available connection.
/// 3. Returns a `deadpool_postgres::Client`.
///
/// If the pool has no available connections:
///
/// - the caller waits until one becomes available
/// - or a new connection is created (if below max_size)
///
/// The returned client is **automatically returned to the pool**
/// when it goes out of scope.
///
/// Example usage:
///
/// ```rust
/// let mut client = get_client().await?;
///
/// let rows = client
///     .query("SELECT * FROM users", &[])
///     .await?;
/// ```
pub async fn get_client() -> Result<deadpool_postgres::Client, DbError> {

    // Retrieve the global pool.
    //
    // If the pool was not initialized yet, return an error.
    let pool = DB_POOL.get().ok_or(DbError::NotInitialized)?;

    // Request a connection from the pool.
    //
    // Internally this may:
    //
    // - reuse an idle connection
    // - validate the connection (`SELECT 1`)
    // - create a new connection if needed
    Ok(pool.get().await?)
}
