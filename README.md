# go-leaderelection

[中文文档](README_zh.md)

A pluggable Go leader election library supporting multiple distributed backends. Designed for multi-instance deployments where a single leader node is needed to perform critical tasks.

## Supported Backends

| Backend | Package | Lock Mechanism |
|---------|---------|----------------|
| **Redis** | `resourcelock/redis` | Distributed lock via [redsync](https://github.com/go-redsync/redsync) |
| **Consul** | `resourcelock/consul` | Session + KV CAS via [consul/api](https://github.com/hashicorp/consul) |
| **etcd** | `resourcelock/etcd` | Lease + KV transaction via [etcd/client](https://go.etcd.io/etcd/client/v3) |
| **ZooKeeper** | `resourcelock/zookeeper` | Ephemeral sequential znodes via [go-zookeeper/zk](https://github.com/go-zookeeper/zk) |

## Features

- Pluggable backend via `resourcelock.Interface`
- Lease renewal and automatic retry
- Callbacks for leader changes, start/stop leading
- Graceful shutdown with `context.Context`
- Async renewal with split-brain detection (stops immediately if lock is definitively lost)
- Per-backend release-on-cancel with timeout protection

## Installation

```bash
go get github.com/hq0101/go-leaderelection
```

## Quick Start

### Redis

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    leaderelection "github.com/hq0101/go-leaderelection"
    redislock "github.com/hq0101/go-leaderelection/resourcelock/redis"
    redis "github.com/redis/go-redis/v9"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    defer client.Close()

    elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
        Lock:            redislock.New(client, "my-election", "node-1", 15*time.Second),
        LeaseDuration:   15 * time.Second,
        RenewDeadline:   10 * time.Second,
        RetryPeriod:     2 * time.Second,
        ReleaseOnCancel: true,
        Callbacks: leaderelection.LeaderCallbacks{
            OnStartedLeading: func(ctx context.Context) {
                log.Println("became leader")
            },
            OnStoppedLeading: func() {
                log.Println("stopped leading")
            },
            OnNewLeader: func(identity string) {
                log.Printf("current leader: %s", identity)
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    elector.Run(ctx)
}
```

### Other Backends

Examples for Consul, etcd, and ZooKeeper are available in the [`example/`](example/) directory.

## Configuration

`LeaderElectionConfig` struct fields:

| Field | Type | Description |
|-------|------|-------------|
| `Lock` | `resourcelock.Interface` | Resource lock implementation (required) |
| `LeaseDuration` | `time.Duration` | Lease duration; the lock expiry time (must be > `RenewDeadline`) |
| `RenewDeadline` | `time.Duration` | Renew deadline; max time to retry refreshing before giving up (must be > `RetryPeriod`) |
| `RetryPeriod` | `time.Duration` | Retry interval between acquire/renew attempts |
| `ReleaseOnCancel` | `bool` | Whether to release the lock when the run context is cancelled |
| `Callbacks` | `LeaderCallbacks` | Lifecycle callbacks |
| `Name` | `string` | Name for debugging purposes |

## Callbacks

| Callback | Signature | Trigger |
|----------|-----------|---------|
| `OnStartedLeading` | `func(context.Context)` | Called when this instance becomes the leader; the context is cancelled when leading stops |
| `OnStoppedLeading` | `func()` | Called when this instance stops leading |
| `OnNewLeader` | `func(identity string)` | Called when a new leader is observed (runs synchronously, must return promptly) |

## API

```go
// Create a leader elector
elector, err := leaderelection.NewLeaderElector(config)

// Run the election (blocks until context is cancelled or leadership is lost)
elector.Run(ctx)

// Check if this instance is the leader
elector.IsLeader() bool

// Get the last observed leader identity (persists after Run returns)
elector.GetLeader() string
```

## Implementing a Custom Backend

Implement `resourcelock.Interface` to add your own backend:

```go
type Interface interface {
    Acquire(ctx context.Context) error
    Renew(ctx context.Context) (bool, error)
    Release(ctx context.Context) (bool, error)
    Identity() string
    CurrentLeader(ctx context.Context) (string, error)
}
```

## Examples

Complete examples with command-line flags are provided in the `example/` directory:

```bash
# Redis
go run example/redis/main.go --redis-addr=localhost:6379 --id=node-1

# Consul
go run example/consul/main.go --consul-addr=localhost:8500 --id=node-1

# etcd
go run example/etcd/main.go --etcd-endpoints=localhost:2379 --id=node-1

# ZooKeeper
go run example/zookeeper/main.go --zk-servers=localhost:2181 --id=node-1
```

Start multiple instances of the same backend to observe leader election and failover.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
