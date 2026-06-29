# example/redis

Redis backend 的 leader election 示例。使用 `redsync` 分布式锁，锁 value 包含 `identity` 和随机 `token`，保证只有持锁者才能释放锁。

## 运行前提

启动一个 Redis 实例（无认证）：

```bash
docker run --rm -p 6379:6379 redis:7
```

或直接使用已有的 Redis 地址。

## 快速启动

在两个终端分别运行：

```bash
go run ./example/redis -id node-a
go run ./example/redis -id node-b
```

两个进程竞争同一把锁，只有一个会成为 leader 并打印心跳日志；另一个持续重试直到获得 leadership。停止当前 leader 后，锁会在 `lease-duration` 到期（或 `release-on-cancel=true` 时立即释放）后转移给下一个竞争者。

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-redis-addr` | `127.0.0.1:6379` | Redis 地址 |
| `-redis-username` | 空 | Redis 用户名（ACL，可选） |
| `-redis-password` | 空 | Redis 密码（可选） |
| `-redis-db` | `0` | Redis DB 编号 |
| `-lock` | `example-leader-election` | 锁名（Redis key） |
| `-id` | `hostname-pid` | 本节点 identity |
| `-lease-duration` | `15s` | 锁过期时间 |
| `-renew-deadline` | `10s` | 续约超时 |
| `-retry-period` | `2s` | 抢锁重试间隔 |
| `-release-on-cancel` | `true` | 退出时主动释放锁 |

## 带认证的示例

Redis 开启传统 `requirepass`：

```bash
go run ./example/redis \
  -redis-addr 127.0.0.1:6379 \
  -redis-password secret \
  -id node-a
```

Redis ACL 用户名 + 密码：

```bash
go run ./example/redis \
  -redis-addr 127.0.0.1:6379 \
  -redis-username myuser \
  -redis-password secret \
  -id node-a
```

## 调整时间参数

```bash
go run ./example/redis \
  -lease-duration 10s \
  -renew-deadline 6s \
  -retry-period 1s \
  -id node-a
```

时间参数约束：`lease-duration > renew-deadline > retry-period > 0`。

## 观察行为

- **正常运行**：leader 每秒打印一条心跳日志，follower 安静等待。
- **停止 leader**：`Ctrl-C` 触发 `SIGTERM`，`release-on-cancel=true` 时立即释放锁，follower 在下一个 `retry-period` 内获得 leadership。
- **Kill leader**：`kill -9` 不会触发主动释放，follower 等待 `lease-duration` 过期后才能抢锁。
- **网络分区**：如果 leader 无法访问 Redis 超过 `renew-deadline`，续约失败，触发 `OnStoppedLeading` 并退出。
