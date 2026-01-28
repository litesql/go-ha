# go-ha

Go database/sql base driver providing high availability for SQLite databases.

## Features

- High availability support for SQLite databases.
- Replication: Synchronize data across nodes using NATS.
- Customize the replication strategy
- Leaderless clusters: Read/Write from/to any node. **Last-writer wins** by default, but you can customize conflict resolutions by implementing *ChangeSetInterceptor*.
- Leader-based cluster: Write operations are redirected to the leader to prevent conflicts entirely.
- Embedded or External NATS: Choose between an embedded NATS server or an external one for replication.
- Easy to integrate with existing Go projects.

## Drivers

- [go-sqlite3-ha](https://github.com/litesql/go-sqlite3-ha): CGO database/sql driver
- [go-sqlite-ha](https://github.com/litesql/go-sqlite-ha): CGO FREE database/sql driver

## Options

| DSN Param | Environment Variable   | Description                                                                                     | Default         |
|------------|---------|---------------------------------------------------------------------------------------|-----------------|
| asyncPublisher          | HA_ASYNC_PUBLISHER | Enables asynchronous publishing of replication events.                                         | false           |
| asyncPublisherOutboxDir | HA_ASYNC_PUBLISHER_OUTBOX_DIR | Directory to store outbox files for asynchronous publishing.                                   |                 |
| autoStart               | HA_AUTO_START | Automatically starts the subscriber and snapshotter when the node is initialized.              | true            |
| replicationID           | HA_REPLICATION_ID | Replication ID | [database filename] |
| deliverPolicy           | HA_DELIVER_POLICY | Specifies the delivery policy for replication events. Options include `all`, `last`, etc.     | all             |
| disableSubscriber    | HA_DISABLE_SUBSCRIBER | Disables the subscriber for replication.                             | false           |
| disablePublisher     | HA_DISABLE_PUBLISHER | Disables the publisher for replication.                              | false           |
| disableDBSnapshotter | HA_DISABLE_SNAPSHOTTER   | Disables the database snapshotter used for initial synchronization.                            | false           |
| disableDDLSync       | HA_DISABLE_DDL_SYNC  | Disables the synchronization of DDL (Data Definition Language) changes across nodes.          | false           |
| grpcPort             | HA_GRPC_PORT   | TCP port for the gRPC server                                       |      |  
| grpcTimeout          | HA_GRPC_TIMEOUT  | Timeout for the gRPC operations                                 | 5s   |
| grpcToken            | HA_GRPC_TOKEN    | Token to protect gRPC server |     |   
| leaderProvider       | HA_LEADER_PROVIDER     | Defines the strategy for determining a leader node in the cluster. This is useful for redirecting HTTP requests. Examples include `dynamic:http://host:port` or `static:http://host:port`.   |       |
| name                 | HA_NAME   | Name of the node in the cluster.                                                   |                 |
| natsConfigFile       | HA_NATS_CONFIG_NAME   | Path to the configuration file for the embedded NATS server. Overrides others NATS configurations.           |                 |
| natsName             | HA_NATS_NAME   | Sets the name of the embedded NATS server.                                                     |                 |
| natsPort             | HA_NATS_PORT   | Configures the port for the embedded NATS server.                                              | 4222            |
| natsStoreDir         | HA_NATS_STORE_DIR   | Directory to store data for the embedded NATS server.                                          |                 |
| publisherTimeout     | HA_PUBLISHER_TIMEOUT   | Timeout duration for publishing replication events.                                            | 15s              |
| replicationStream    | HA_REPLICATION_STREAM   | Name of the replication stream used for synchronizing data.                                    |                 |
| replicationURL       | HA_REPLICATION_URL   | URL used for connecting to the replication stream.                                             |                 |
| replicas             | HA_REPLICAS   | Number of replicas to maintain for high availability.                                          | 1               |
| rowIdentify          | HA_ROW_IDENTIDY   | Row identify strategy: pk, rowid or full | pk |
| snapshotInterval     | HA_SNAPSHOT_INTERVAL   | Interval for taking database snapshots.                                                        | 1m              |
| streamMaxAge         | HA_STREAM_MAX_AGE   | Maximum age of messages in the replication stream before they are removed.                     |              |


## Projects using go-ha

- [HA](https://github.com/litesql/ha): Highly available leaderless SQLite cluster with HTTP and PostgreSQL Wire Protocol
- [PocketBase HA](https://github.com/litesql/pocketbase-ha): Highly available leaderless PocketBase cluster
- [sqlc-http](https://github.com/walterwanderley/sqlc-http): Generate net/http go server from SQL

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.