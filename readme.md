# MIT 6.824

MIT 6.824/6.5840课程项目，分布式分片KV缓存

一共4个Lab。已完成前3个Lab，Lab4还在debug

## Lab1
实现一个分布式 MapReduce，由两个程序组成：coordinator和worker。只有一个coordinator进程，以及一个或多个并行执行的worker进程。worker将通过 RPC 与coordinator通信。每个worker进程都会向coordinator请求一项任务，从一个或多个文件读取任务的输入，执行任务，并将任务的输出写入一个或多个文件。coordinator应该注意到某个worker是否未在合理的时间内完成其任务（对于本实验，使用十秒），并将相同的任务分配给不同的worker。

## Lab2
任务：使用go语言实现raft。参考论文 raft-extended，需要实现除了集群成员变更之外的绝大部分内容。
Raft 将客户端请求组织成一个序列（称为日志），并确保所有副本服务器有相同的日志。每个副本按日志顺序执行客户端请求，将它们应用到服务状态的本地副本。由于所有实时副本都看到相同的日志内容，因此它们都以相同的顺序执行相同的请求，从而具有相同的服务状态。如果服务器发生故障但后来恢复，Raft 会负责更新其日志。只要至少大多数服务器处于活动状态并且可以相互通信，Raft 就会继续运行。如果没有这样的多数，Raft 将不会取得任何进展，但一旦多数可以再次通信，Raft 就会从上次中断的地方继续。
1. Lab2A：Leader 选出单个领导者，如果没有故障，领导者将继续担任领导者；如果旧领导者出现故障，或者发往/来自旧领导者的数据包不可用，则新的领导者将接管。
2. Lab2B: log. 完成leader和follower代码以实现AppendEntry日志复制
3. Lab2C: persistence. 如果基于 Raft 的服务器重新启动，它应该从中断处恢复服务。
4. Lab2D: log compaction.
## Lab3
lab3 的内容是要在 lab2 的基础上实现一个高可用的 KV 存储服务，算是要将 raft 真正的用起来。在该任务中，要处理一个很重要的事情，就是线性化语义，也可以要求每个请求要具有幂等性。
1. Lab3A: Key/value service without snapshots
2. Lab3B: Key/value service with snapshots
## Lab4
在本实验中，我们将构建一个带分片的KV缓存，即一组副本组上的键。每一个分片都是KV缓存的子集。一开始系统会创建一个 shardctrler 组来负责配置更新，分片分配等任务，接着系统会创建多个 raft 组来承载所有分片的读写任务。
1. Lab4A: The Shard controller. 4A主要就是实现shardctrler，其实它就是一个高可用的集群配置管理服务。它主要记录了当前整个系统的配置信息Config，即每组中各个节点的 servername 以及当前每个 shard 被分配到了哪个组。
2. Lab4B: Sharded Key/Value Server. 相比于KVRaft，ShardKV增加了分片的概念，可以在运行中增加或者减少分片。每一个Raft组的Leader需要监控集群配置的变化，并将新配置应用在该Raft组，包括分片迁移，分片清理等，并同时保证线性一致性。