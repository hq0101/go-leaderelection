# go-leaderelection

[中文文档](README_zh.md)

A Go leader election library based on Redis distributed locks, designed for multi-instance deployments where a single leader node is needed to perform critical tasks.

## Features

- Built on Redis distributed locks via [redsync](https://github.com/go-redsync/redsync)
- Lease renewal and automatic retry
- Callbacks for leader changes, start/stop leading
- Graceful shutdown with `context.Context`
- Supports `redis.UniversalClient` — compatible with standalone, Sentinel, and Cluster modes

## Installation

```bash
go get github.com/hq0101/go-leaderelection
```

## Quick Start

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
    redis "github.com/redis/go-redis/v9"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "",
        DB:       0,
    })
    defer client.Close()

    elector, err := leaderelection.NewLeaderElector(leaderelection.Config{
        LockName:        "my-leader-election",
        Identity:        "node-1",
        LeaseDuration:   15 * time.Second,
        RenewDeadline:   10 * time.Second,
        RetryPeriod:     2 * time.Second,
        ReleaseOnCancel: true,
        RedisClient:     client,
        Callbacks: leaderelection.Callbacks{
            OnStartedLeading: func(ctx context.Context) {
                log.Println("became leader, starting work")
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
        log.Fatalf("create leader elector: %v", err)
    }

    elector.Run(ctx)
}
```

## Configuration

`Config` struct fields:

| Field | Type | Description |
|-------|------|-------------|
| `LockName` | `string` | Lock name; instances sharing the same lock name compete in the same election |
| `Identity` | `string` | Unique identifier for this instance, e.g. hostname + PID |
| `LeaseDuration` | `time.Duration` | Lease duration; the lock expiry time |
| `RenewDeadline` | `time.Duration` | Renew deadline; must be less than `LeaseDuration` |
| `RetryPeriod` | `time.Duration` | Retry interval; must be less than or equal to `RenewDeadline` |
| `ReleaseOnCancel` | `bool` | Whether to release the lock when the context is cancelled |
| `RedisClient` | `redis.UniversalClient` | Redis client instance |
| `Callbacks` | `Callbacks` | Leader state change callbacks |

## Callbacks

| Callback | Signature | Trigger |
|----------|-----------|---------|
| `OnStartedLeading` | `func(context.Context)` | Called when this instance becomes the leader; the context is cancelled when leading stops |
| `OnStoppedLeading` | `func()` | Called when this instance stops leading |
| `OnNewLeader` | `func(identity string)` | Called when a new leader is observed |

## API

```go
// Create a leader elector
elector, err := leaderelection.NewLeaderElector(config)

// Run the election (blocks until context is cancelled)
elector.Run(ctx)

// Check if this instance is the leader
elector.IsLeader() bool

// Get the current leader identity
elector.GetLeader() string
```

## Example

A complete example is provided in the `example/` directory with command-line flags:

```bash
cd example
go run main.go --redis-addr=localhost:6379 --id=node-1 --lock=example-leader-election
```

Start multiple instances to observe the leader election and failover process.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
