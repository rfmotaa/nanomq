# nano-kafka

A tiny append-only messaging broker, built to explore Java and the ideas behind Kafka.

## Model

- A **topic** is an append-only log: one file, one record per line.
- A record's **offset** is its zero-based line number — a stable, monotonic position.
- The broker is **stateless for reads**: a consumer asks for an offset and gets back the
  record plus the `nextOffset` to use. The consumer tracks its own position.
- Topics survive restarts (they are just files); the broker rebuilds what it needs on boot.

## Wire protocol

Newline-delimited JSON over TCP: one request per line, one response per line.

| Request | Response on success |
|---|---|
| `{"action":"CREATE","topic":"orders"}` | `{"status":"OK","message":"created topic orders"}` |
| `{"action":"WRITE","topic":"orders","payload":"hello"}` | `{"status":"OK","message":"stored at offset 0","offset":0,"nextOffset":1}` |
| `{"action":"READ","topic":"orders","offset":0}` | `{"status":"OK","message":"hello","offset":0,"nextOffset":1,"endOfLog":false}` |
| `READ` past the end | `{"status":"OK","nextOffset":0,"endOfLog":true}` |

Any failure: `{"status":"ERROR","message":"..."}`. A bad request never drops the connection.

## Run

```bash
mvn compile

# broker (port 8080, data in ./data by default)
mvn exec:java -Dexec.mainClass=broker.BrokerServer

# in other terminals
mvn exec:java -Dexec.mainClass=client.Producer
mvn exec:java -Dexec.mainClass=client.Consumer

mvn test
```

Override with `-Dbroker.port=9000 -Dbroker.dataDir=/tmp/nano-kafka`.

## Layout

```
broker/
  BrokerServer        accept loop + thread pool
  BrokerException     client-safe errors
  network/            one ConnectionHandler per connection
  protocol/           Request / Response DTOs
  persistence/        TopicLog — append / readAt
client/
  BrokerClient        shared transport
  Producer, Consumer  demos
```

## Natural next steps

- Broker-side offset commit (consumer groups).
- Segment files + a sparse index instead of one growing file read whole.
- Long-polling `READ` so consumers block until a record arrives.