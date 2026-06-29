# example/etcd

etcd backend 的 leader election 示例。使用 etcd lease + KV txn 实现互斥，leadership 生命周期直接绑定到 etcd lease。

## 运行前提

启动一个单节点 etcd：

```bash
docker run --rm -p 2379:2379 \
  quay.io/coreos/etcd:v3.5.0 \
  etcd \
  --advertise-client-urls http://0.0.0.0:2379 \
  --listen-client-urls http://0.0.0.0:2379
```

或使用已有的 etcd 集群地址。

## 快速启动

在两个终端分别运行：

```bash
go run ./example/etcd -id node-a
go run ./example/etcd -id node-b
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-etcd-endpoints` | `127.0.0.1:2379` | etcd 地址，多个用逗号分隔 |
| `-lock` | `example-leader-election` | 锁名（etcd key） |
| `-id` | `hostname-pid` | 本节点 identity |
| `-lease-duration` | `15s` | lease TTL（向上取整到秒） |
| `-renew-deadline` | `10s` | 续约超时 |
| `-retry-period` | `2s` | 抢锁重试间隔 |
| `-release-on-cancel` | `true` | 退出时 revoke lease 并释放 leadership |

## 多节点 etcd 示例

```bash
go run ./example/etcd \
  -etcd-endpoints 10.0.0.1:2379,10.0.0.2:2379,10.0.0.3:2379 \
  -id node-a
```

## lease TTL 与 LeaseDuration

etcd lease TTL 只支持整秒。`-lease-duration` 值会向上取整：

| 传入值 | 实际 lease TTL |
|--------|----------------|
| `15s` | `15s` |
| `1500ms` | `2s` |
| `10500ms` | `11s` |

建议直接传整秒值以避免误解。

## 观察行为

- **停止 leader（Ctrl-C）**：`release-on-cancel=true` 时 revoke lease，follower 立即抢锁。
- **Kill leader**：lease 在 `lease-duration` 过期后自动被 etcd 服务端删除，follower 等待过期后抢锁。
- **etcd 不可达**：leader 无法续约，超过 `renew-deadline` 后触发 `OnStoppedLeading` 并退出。
