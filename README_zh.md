# go-leaderelection

[English](README.md)

可插拔的 Go 领导者选举库，支持多种分布式后端。适用于多实例部署场景下选出唯一 Leader 节点执行关键任务。

## 支持的后端

| 后端 | 包路径 | 锁机制 |
|------|--------|--------|
| **Redis** | `resourcelock/redis` | 基于 [redsync](https://github.com/go-redsync/redsync) 的分布式锁 |
| **Consul** | `resourcelock/consul` | 基于 [consul/api](https://github.com/hashicorp/consul) 的 Session + KV CAS |
| **etcd** | `resourcelock/etcd` | 基于 [etcd/client](https://go.etcd.io/etcd/client/v3) 的 Lease + KV 事务 |
| **ZooKeeper** | `resourcelock/zookeeper` | 基于 [go-zookeeper/zk](https://github.com/go-zookeeper/zk) 的临时顺序节点 |

## 特性

- 通过 `resourcelock.Interface` 实现可插拔后端
- 租约续约和自动重试
- 领导者变更、开始/停止领导等回调
- 支持 `context.Context` 优雅退出
- 异步续约并检测脑裂（锁确定丢失时立即停止）
- 退出时释放锁带有超时保护

## 安装

```bash
go get github.com/hq0101/go-leaderelection
```

## 快速开始

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
                log.Println("成为 Leader")
            },
            OnStoppedLeading: func() {
                log.Println("停止领导")
            },
            OnNewLeader: func(identity string) {
                log.Printf("当前 Leader: %s", identity)
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    elector.Run(ctx)
}
```

### 其他后端

Consul、etcd、ZooKeeper 的完整示例请参见 [`example/`](example/) 目录。

## 配置说明

`LeaderElectionConfig` 结构体字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `Lock` | `resourcelock.Interface` | 资源锁实现（必填） |
| `LeaseDuration` | `time.Duration` | 租约时长，必须大于 `RenewDeadline` |
| `RenewDeadline` | `time.Duration` | 续约截止时间，必须大于 `RetryPeriod` |
| `RetryPeriod` | `time.Duration` | 重试间隔 |
| `ReleaseOnCancel` | `bool` | 当运行 context 取消时是否主动释放锁 |
| `Callbacks` | `LeaderCallbacks` | 生命周期回调 |
| `Name` | `string` | 用于调试的名称 |

## 回调说明

| 回调 | 签名 | 触发时机 |
|------|------|----------|
| `OnStartedLeading` | `func(context.Context)` | 当前实例成为 Leader 时触发，context 取消时结束领导 |
| `OnStoppedLeading` | `func()` | 当前实例停止领导时触发 |
| `OnNewLeader` | `func(identity string)` | 观察到新 Leader 时触发（同步调用，必须尽快返回） |

## API

```go
// 创建领导者选举器
elector, err := leaderelection.NewLeaderElector(config)

// 启动选举（阻塞运行，直到 context 取消或领导权丢失）
elector.Run(ctx)

// 查询当前是否为 Leader
elector.IsLeader() bool

// 获取最后观察到的 Leader 标识（Run 返回后仍可读取）
elector.GetLeader() string
```

## 自定义后端

实现 `resourcelock.Interface` 即可添加自定义后端：

```go
type Interface interface {
    Acquire(ctx context.Context) error
    Renew(ctx context.Context) (bool, error)
    Release(ctx context.Context) (bool, error)
    Identity() string
    CurrentLeader(ctx context.Context) (string, error)
}
```

## 示例

项目 `example/` 目录提供了各后端的完整示例，支持命令行参数配置：

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

启动多个实例即可观察领导者选举和切换过程。

## 许可证

本项目基于 MIT 许可证开源，详见 [LICENSE](LICENSE)。
