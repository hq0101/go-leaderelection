# example/consul

Consul backend 的 leader election 示例。使用 Consul Session + KV Acquire 实现互斥，session TTL 映射为 `lease-duration`。

## 运行前提

启动一个 Consul agent（开发模式）：

```bash
docker run --rm -p 8500:8500 consul:1.15 agent -dev -client 0.0.0.0
```

或使用已有的 Consul 地址。

## 快速启动

在两个终端分别运行：

```bash
go run ./example/consul -id node-a
go run ./example/consul -id node-b
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-consul-addr` | `127.0.0.1:8500` | Consul HTTP 地址 |
| `-consul-token` | 空 | Consul ACL token（可选） |
| `-lock` | `example-leader-election` | 锁名（Consul KV key） |
| `-id` | `hostname-pid` | 本节点 identity |
| `-lease-duration` | `15s` | Consul session TTL（最小 10s） |
| `-renew-deadline` | `10s` | 续约超时 |
| `-retry-period` | `2s` | 抢锁重试间隔 |
| `-release-on-cancel` | `true` | 退出时销毁 session 并释放锁 |

## 带 ACL Token 的示例

```bash
go run ./example/consul \
  -consul-addr 127.0.0.1:8500 \
  -consul-token my-acl-token \
  -id node-a
```

## Consul session TTL 限制

Consul 要求 session TTL 最小 10s，最大 86400s。如果 `-lease-duration` 小于 10s，程序会在启动时报错退出：

```
create consul lock: lease duration must be >= 10s for Consul backend
```

## LockDelay

backend 创建 session 时 `LockDelay` 设为 0，session 失效后新 leader 可立即获得 KV lock，无需等待。

## 观察行为

- **停止 leader（Ctrl-C）**：销毁 session，KV key 由 `SessionBehaviorDelete` 自动删除，follower 立即抢锁。
- **Kill leader**：session TTL 到期后 Consul 服务端自动销毁 session 并删除 KV key，follower 等待 TTL 过期后抢锁。
- **Consul 不可达**：leader 无法续期 session，超过 `renew-deadline` 后触发 `OnStoppedLeading` 并退出。
