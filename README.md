# go-ha

[![Go Reference](https://pkg.go.dev/badge/github.com/litesql/go-ha.svg)](https://pkg.go.dev/github.com/litesql/go-ha)
[![Go Report Card](https://goreportcard.com/badge/github.com/litesql/go-ha)](https://goreportcard.com/report/github.com/litesql/go-ha)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Go `database/sql` base driver providing high availability for SQLite databases through NATS-based replication.

## Features

- **High Availability**: Ensure your SQLite databases remain accessible and consistent across multiple nodes.
- **Replication**: Synchronize data across nodes using NATS messaging.
- **Flexible Conflict Resolution**: Customize replication strategies and handle conflicts with `ChangeSetInterceptor`.
- **Cluster Modes**:
  - **Leaderless**: Read/write from any node with last-writer-wins resolution.
  - **Leader-based**: Redirect writes to a leader to prevent conflicts.
- **NATS Integration**: Choose between embedded or external NATS servers.
- **Seamless Integration**: Drop-in replacement for existing Go `database/sql` usage.
- **gRPC Support**: Expose database operations via gRPC for remote access the sqlite database.
- **Snapshotting**: Automatic database snapshots for efficient synchronization.
- **Cross-database Queries**: Transparently execute cross-shard queries using SQL hints `/*+ db=DSN */`.
- **Transaction Undo**: Revert already committed transactions.

## Architecture

go-ha enables high availability for SQLite by replicating changes across multiple nodes using NATS as the messaging backbone. Each node maintains a local SQLite database and publishes changes to a NATS stream. Other nodes subscribe to this stream and apply the changes, ensuring data consistency.

## Installation

```bash
go get github.com/litesql/go-ha
```

## Quick Start

### Basic Usage

### Instance 1

```go
package main

import (
    "database/sql"
    "github.com/litesql/go-ha"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
)

func main() {
    slog.SetLogLoggerLevel(slog.LevelDebug)
    // Open a connection with HA enabled
    c, err := sqlite3ha.NewConnector("file:my.db?_journal=WAL&_timeout=5000",
		ha.WithName("instance1"),
        ha.WithReplicationID("example"),
		ha.WithGrpcPort(5000),		
		ha.WithEmbeddedNatsConfig(&ha.EmbeddedNatsConfig{
			Port: 4222,
		}))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	db := sql.OpenDB(c)
	defer db.Close()

    // Use like any other database/sql driver
    _, err = db.Exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
    if err != nil {
        panic(err)
    }

    // Insert data
    _, err = db.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
    if err != nil {
        panic(err)
    }
}
```

### Instance 2 (leader-based)

```go
package main

import (
    "database/sql"
    "github.com/litesql/go-ha"
	sqlite3ha "github.com/litesql/go-sqlite3-ha"
)

func main() {
    slog.SetLogLoggerLevel(slog.LevelDebug)
    db, err := sql.Open("sqlite3-ha", "file:my2.db?_journal=WAL&_timeout=5000&replicationURL=nats://localhost:4222&replicationID=example&name=instance2&grpcInsecure=true&leaderProvider=static:localhost:5000")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec("INSERT INTO users(name) values('grpc leader redirect')")
	if err != nil {
		panic(err)
	}

	var name string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM users ORDER BY rowid desc LIMIT 1").Scan(&name)
	if err != nil {
		panic(err)
	}

	fmt.Println("User:", name)
}
```

### Cross-Database Queries

Execute queries across multiple databases using SQL hints:

```go
// Query data from all loaded databases from the driver
rows, err := db.Query("/*+ db=.* */ SELECT * FROM users WHERE id = ?", 1)
if err != nil {
    panic(err)
}
defer rows.Close()
```

## Drivers

go-ha provides several driver implementations:

- **[go-sqlite3-ha](https://github.com/litesql/go-sqlite3-ha)**: CGO-based driver using `go-sqlite3`.
- **[go-sqlite-ha](https://github.com/litesql/go-sqlite-ha)**: CGO-free driver using `modernc.org/sqlite`.
- **[go-remote-ha](https://github.com/litesql/go-remote-ha)**: Remote-only driver for accessing HA clusters via gRPC.

## Configuration Options

| DSN Parameter          | Description                                                                 | Default         |
|------------------------|-----------------------------------------------------------------------------|-----------------|
| asyncPublisher         | Enables asynchronous publishing of replication events.                      | false           |
| asyncPublisherOutboxDir| Directory to store outbox files for asynchronous publishing.                |                 |
| autoStart              | Automatically starts subscriber and snapshotter on initialization.         | true            |
| replicationID          | Unique ID for this replication instance.                                    | [database filename] |
| deliverPolicy          | Delivery policy for replication events (`all`, `last`, etc.).              | all             |
| disableSubscriber      | Disables the replication subscriber.                                        | false           |
| disablePublisher       | Disables the replication publisher.                                         | false           |
| disableDBSnapshotter   | Disables database snapshotter for initial sync.                             | false           |
| disableDDLSync         | Disables synchronization of DDL changes.                                    | false           |
| grpcInsecure           | Use insecure gRPC connections.                                              | false           |
| grpcPort               | TCP port for gRPC server.                                                   |                 |
| grpcTimeout            | Timeout for gRPC operations.                                                | 5s              |
| grpcToken              | Authentication token for gRPC server.                                       |                 |
| leaderProvider         | Leader election strategy (e.g., `dynamic:local-host:port`, `static:remote-host:port`).                |                 |
| name                   | Node name in the cluster.                                                   |                 |
| natsConfigFile         | Path to NATS server config file.                                            |                 |
| natsName               | Name for embedded NATS server.                                              |                 |
| natsPort               | Port for embedded NATS server.                                              | 4222            |
| natsStoreDir           | Data directory for embedded NATS server.                                    |                 |
| publisherTimeout       | Timeout for publishing replication events.                                  | 15s             |
| replicationStream      | Name of the NATS stream for replication.                                    |                 |
| replicationURL         | NATS server URL for replication.                                            |                 |
| replicas               | Number of replicas for high availability.                                   | 1               |
| rowIdentify            | Row identification strategy: `pk`, `rowid`, or `full`.                      | pk              |
| snapshotInterval       | Interval between database snapshots.                                        | 1m              |
| streamMaxAge           | Maximum age of messages in replication stream.                              |                 |


## Performance Considerations

- Use embedded NATS for single-machine deployments to reduce latency.
- Configure `snapshotInterval` based on your write frequency.
- For high-throughput scenarios, consider leader-based clusters to avoid conflicts.
- Monitor NATS stream size and adjust `streamMaxAge` to prevent unbounded growth.

## Troubleshooting

### Common Issues

- **Replication not working**: Ensure NATS server is running and accessible via `replicationURL`.
- **Conflicts in leaderless mode**: Implement a custom `ChangeSetInterceptor` for complex conflict resolution.
- **Slow synchronization**: Check snapshotter configuration and network latency.
- **gRPC connection errors**: Verify `grpcPort` and `grpcToken` settings.


## Projects Using go-ha

- **[HA](https://github.com/litesql/ha)**: Highly available leaderless SQLite cluster with HTTP and PostgreSQL Wire Protocol
- **[PocketBase HA](https://github.com/litesql/pocketbase-ha)**: Highly available leaderless PocketBase cluster
- **[sqlc-http](https://github.com/walterwanderley/sqlc-http)**: Generate net/http Go server from SQL

## Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

For major changes, please open an issue first to discuss the proposed changes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.