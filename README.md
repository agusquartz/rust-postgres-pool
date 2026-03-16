# rust-postgres-pool

A minimal and well-documented example of how to implement a **PostgreSQL connection pool in Rust** using:

- `deadpool-postgres`
- `tokio-postgres`
- `tokio`
- `once_cell`

The goal of this repository is to provide a **simple reference implementation** showing how to create and use a **global asynchronous PostgreSQL connection pool** in Rust applications.

This project is intentionally small and heavily documented so it can be used as a learning resource or a starting point for real backend services.

---

## Features

- Global connection pool using `OnceCell`
- Asynchronous PostgreSQL access with `tokio`
- Automatic connection recycling
- TCP keepalive configuration
- Clean error handling with `thiserror`
- Examples of both **transactional** and **non-transactional** queries

The implementation avoids unnecessary complexity while still following good production practices.

---

## Architecture

The project contains two main files:

```
src/
 ├─ main.rs
 └─ db_config.rs
```

### `db_config.rs`

Responsible for:

- creating the PostgreSQL configuration
- initializing the connection pool
- exposing a global `get_client()` function
- managing database errors

The connection pool is stored in a global `OnceCell`.

This guarantees:

- single initialization
- thread safety
- lock-free reads

### `main.rs`

Demonstrates how to:

1. Initialize the pool
2. Run a simple `SELECT` query
3. Execute queries inside a transaction
4. Commit or rollback based on errors

---

## How the Connection Pool Works

Instead of using a single database connection protected by a mutex, this project uses a **connection pool**.

A pool maintains multiple open connections and distributes them to tasks when needed.

Typical flow:

```
application
    │
    ▼
get_client()
    │
    ▼
connection pool
    │
    ▼
available connection
```

When the client goes out of scope, the connection is automatically returned to the pool.

This allows **concurrent database operations without locking**.

---

## Connection Validation

The pool uses:

```
RecyclingMethod::Verified
```

Before returning a connection to the application, the pool executes:

```
SELECT 1
```

If the query fails, the connection is discarded and replaced automatically.

This prevents the application from receiving **stale or broken connections**.

---

## TCP Keepalive

TCP keepalive is enabled to help detect broken network connections.

This protects against situations like:

- router silently dropping connections
- firewalls killing idle connections
- network interruptions
- laptop sleep states

When a connection becomes invalid, the pool recreates it automatically.

---

## Running the Example

### 1. Clone the repository

```
git clone https://github.com/agusquartz/rust-postgres-pool.git
cd rust-postgres-pool
```

### 2. Configure the database

The application reads database configuration from environment variables.

Instead of relying on the current working directory, the program explicitly loads the environment file from the standard OS config directory. This makes the configuration more reliable across platforms.

Create a file named `.env` inside the `rust-postgres-pool` directory in your OS config folder:
- Linux: ~/.config/rust-postgres-pool/.env
- macOS: ~/Library/Application Support/rust-postgres-pool/.env
- Windows: C:\Users\<User>\AppData\Roaming\rust-postgres-pool\.env

The configuration file is stored inside a directory named after the application
(`rust-postgres-pool`) within the OS configuration directory.
This avoids conflicts with other applications and follows standard platform
conventions.

Example .env:

```
DB_HOST=127.0.0.1
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=your_database
DB_POOL_SIZE=16
```

The project uses the dotenvy crate to load the environment file.

Example configuration in main.rs:

```rust
use dotenvy::from_filename;
use std::env;
use dirs;

// Determine the OS-specific config directory
let mut path = dirs::config_dir()
    .ok_or("Cannot find configuration directory")?;

// Append the path to the env file
path.push("rust-postgres-pool/.env");

// Load environment variables from the file
from_filename(&path)?;

// Initialize the database parameters
let params = db_config::DbParams {
    host: env::var("DB_HOST")?,
    port: env::var("DB_PORT")?.parse()?,
    user: env::var("DB_USER")?,
    password: env::var("DB_PASSWORD")?,
    db_name: Some(env::var("DB_NAME")?),
    pool_max_size: env::var("DB_POOL_SIZE")?.parse()?,
};
```

This approach ensures that the application always loads the configuration file from the standard OS config directory, regardless of the working directory or where the binary is executed.

### 3. Run the project

```
cargo run
```

---

## Dependencies

```
tokio
tokio-postgres
deadpool-postgres
once_cell
thiserror
dotenvy
dirs
```

---

## When to Use Transactions

Transactions are useful when:

- multiple writes must succeed or fail together
- performing read → modify → write workflows
- maintaining consistent reads
- implementing financial or critical operations

For simple read queries, explicit transactions are usually unnecessary because PostgreSQL automatically wraps each statement in its own implicit transaction.

---

## Purpose of This Repository

This project is meant to serve as:

- a **learning example**
- a **reference implementation**
- a **starting template** for Rust backend services using PostgreSQL

It focuses on clarity, documentation, and correctness rather than advanced abstractions.

---

## License

MIT
