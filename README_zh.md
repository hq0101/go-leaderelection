# go-leaderelection

[English](README.md)

基于 Redis 分布式锁实现的 Go 领导者选举库，适用于多实例部署场景下选出唯一 Leader 节点执行关键任务。

## 特性

- 基于 Redis 分布式锁（[redsync](https://github.com/go-redsync/redsync)）实现
- 支持租约续约、自动重试
- 提供领导者变更、开始/停止领导等回调
- 支持 `context.Context` 优雅退出
- 支持 `redis.UniversalClient`，兼容单机、Sentinel、Cluster 模式

## 安装

```bash
go get github.com/hq0101/go-leaderelection
```

## 快速开始

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
                log.Println("成为 Leader，开始执行任务")
            },
            OnStoppedLeading: func() {
                log.Println("不再是 Leader")
            },
            OnNewLeader: func(identity string) {
                log.Printf("当前 Leader: %s", identity)
            },
        },
    })
    if err != nil {
        log.Fatalf("创建选举器失败: %v", err)
    }

    elector.Run(ctx)
}
```

## 配置说明

`Config` 结构体字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `LockName` | `string` | 锁名称，同一锁名称的实例参与同一选举 |
| `Identity` | `string` | 当前实例唯一标识，如主机名+PID |
| `LeaseDuration` | `time.Duration` | 租约时长，锁的过期时间 |
| `RenewDeadline` | `time.Duration` | 续约截止时间，必须小于 `LeaseDuration` |
| `RetryPeriod` | `time.Duration` | 重试间隔，必须小于等于 `RenewDeadline` |
| `ReleaseOnCancel` | `bool` | 当 context 取消时是否主动释放锁 |
| `RedisClient` | `redis.UniversalClient` | Redis 客户端实例 |
| `Callbacks` | `Callbacks` | 领导状态变更回调 |

## 回调说明

| 回调 | 签名 | 触发时机 |
|------|------|----------|
| `OnStartedLeading` | `func(context.Context)` | 当前实例成为 Leader 时触发，context 取消时结束领导 |
| `OnStoppedLeading` | `func()` | 当前实例停止领导时触发 |
| `OnNewLeader` | `func(identity string)` | 观察到新 Leader 时触发 |

## API

```go
// 创建领导者选举器
elector, err := leaderelection.NewLeaderElector(config)

// 启动选举（阻塞运行，直到 context 取消）
elector.Run(ctx)

// 查询当前是否为 Leader
elector.IsLeader() bool

// 获取当前 Leader 标识
elector.GetLeader() string
```

## 示例

项目 `example/` 目录提供了完整示例，支持通过命令行参数配置：

```bash
cd example
go run main.go --redis-addr=localhost:6379 --id=node-1 --lock=example-leader-election
```

启动多个实例即可观察领导者选举和切换过程。

## 许可证

本项目基于 MIT 许可证开源，详见 [LICENSE](LICENSE)。
