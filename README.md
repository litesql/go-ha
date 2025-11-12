# go-ha

Go database/sql base driver providing high availability for SQLite databases.

## Features

- High availability support for SQLite databases.
- Replication: Synchronize data across nodes using NATS.
- Customize the replication strategy
- Leaderless clusters: Read/Write from/to any node. **Last-writer wins** by default, but you can customize conflict resolutions by implementing *ChangeSetInterceptor*.
- Embedded or External NATS: Choose between an embedded NATS server or an external one for replication.
- Easy to integrate with existing Go projects.

## Drivers

- [go-sqlite3-ha](https://github.com/litesql/go-sqlite3-ha): CGO database/sql driver
- [go-sqlite-ha](https://github.com/litesql/go-sqlite-ha): CGO FREE database/sql driver

## Options

| Option                  | Description                                                                                     | Default         |
|-------------------------|-------------------------------------------------------------------------------------------------|-----------------|
| asyncPublisher          | Enables asynchronous publishing of replication events.                                         | false           |
| asyncPublisherOutboxDir | Directory to store outbox files for asynchronous publishing.                                   |                 |
| autoStart               | Automatically starts the subscriber and snapshotter when the node is initialized.              | true            |
| cdcID                   | Change Data Captures ID | [database filename] |
| deliverPolicy           | Specifies the delivery policy for replication events. Options include `all`, `last`, etc.     | all             |
| disableCDCSubscriber    | Disables the Change Data Capture (CDC) subscriber for replication.                             | false           |
| disableCDCPublisher     | Disables the Change Data Capture (CDC) publisher for replication.                              | false           |
| disableDBSnapshotter    | Disables the database snapshotter used for initial synchronization.                            | false           |
| disableDDLSync          | Disables the synchronization of DDL (Data Definition Language) changes across nodes.          | false           |
| leaderTarget            | Specifies the target node to act as the leader in the cluster. Used to redirect HTTP requests.   |       |
| name                    | Specifies the name of the node in the cluster.                                                 |                 |
| natsConfigFile          | Path to the configuration file for the embedded NATS server. Overrides others NATS configurations.           |                 |
| natsName                | Sets the name of the embedded NATS server.                                                     |                 |
| natsPort                | Configures the port for the embedded NATS server.                                              | 4222            |
| natsStoreDir            | Directory to store data for the embedded NATS server.                                          |                 |
| publisherTimeout        | Timeout duration for publishing replication events.                                            | 5s              |
| replicationStream       | Name of the replication stream used for synchronizing data.                                    |                 |
| replicationURL          | URL used for connecting to the replication stream.                                             |                 |
| replicas                | Number of replicas to maintain for high availability.                                          | 1               |
| rowIdentify             | Row identify strategy: rowid or full | rowid |
| snapshotInterval        | Interval for taking database snapshots.                                                        | 1m              |
| streamMaxAge            | Maximum age of messages in the replication stream before they are removed.                     |              |


## Projects using go-ha

- [HA](https://github.com/litesql/ha): Highly available leaderless SQLite cluster with HTTP and PostgreSQL Wire Protocol
- [PocketBase HA](https://github.com/litesql/pocketbase-ha): Highly available leaderless PocketBase cluster 

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.