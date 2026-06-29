# example/zookeeper

ZooKeeper backend 的 leader election 示例。使用 ephemeral sequential znode 实现选主，leadership 生命周期由 ZooKeeper session timeout 决定。

## 运行前提

启动一个单节点 ZooKeeper：

```bash
docker run --rm -p 2181:2181 zookeeper:3.8
```

或使用已有的 ZooKeeper 地址。

## 快速启动

在两个终端分别运行：

```bash
go run ./example/zookeeper -id node-a
go run ./example/zookeeper -id node-b
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-zk-servers` | `127.0.0.1:2181` | ZooKeeper 地址，多个用逗号分隔 |
| `-zk-session-timeout` | `15s` | ZooKeeper session timeout（同时用作 LeaseDuration） |
| `-lock` | `example-leader-election` | 锁名（znode 目录名，不含前导 `/`） |
| `-id` | `hostname-pid` | 本节点 identity |
| `-renew-deadline` | `10s` | 续约超时 |
| `-retry-period` | `2s` | 抢锁重试间隔 |
| `-release-on-cancel` | `true` | 退出时删除候选节点 |

## ZooKeeper ensemble 示例

```bash
go run ./example/zookeeper \
  -zk-servers 10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181 \
  -zk-session-timeout 15s \
  -id node-a
```

## session timeout 与 leaseDuration

ZooKeeper backend 不使用 `leaseDuration` 参数控制 leadership 生命周期——ephemeral znode 由 ZooKeeper session 保活，session timeout 在 `zk.Connect` 时决定。示例程序将 `-zk-session-timeout` 同时传给 `zk.Connect` 和 `LeaderElectionConfig.LeaseDuration`，保持两者一致。

如果修改了其中一个参数，请同时修改另一个，否则时间参数校验（`LeaseDuration > RenewDeadline`）可能不匹配实际的 session 行为。

## 观察行为

- **停止 leader（Ctrl-C）**：`release-on-cancel=true` 时删除候选 znode，follower 立即获得 leadership。
- **Kill leader**：ZooKeeper session 到期（`zk-session-timeout`）后 ephemeral znode 自动删除，follower 等待超时后抢锁。
- **ZooKeeper 不可达**：leader 无法联系 ZK 服务器，`Renew` 返回错误，超过 `renew-deadline` 后触发 `OnStoppedLeading` 并退出。

## 选主机制说明

每个竞争者在 `/<lockname>/` 下创建 `candidate-` 前缀的 sequential 临时节点（例如 `candidate-0000000001`）。sequence 编号最小的节点对应的进程为 leader。`Renew` 仅检查当前节点是否仍存在且编号最小，不写入任何数据，因此对 ZooKeeper 服务端的压力极低。
