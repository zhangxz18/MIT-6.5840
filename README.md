# My solution for MIT 6.5840


## Notes
### Lab 1
+ 记得在每次发送RPC前重置Reply。因为空值在RPC里会被压缩。如果不重置，server往reply里写的0会被压缩掉，client会在收到reply后保持原有值。

+ 用-race跑会报一些读写冲突，但不影响正确性。（只是可能分配已经完成的任务出去）

### Lab2
+ bug: 如果把ticker的唤醒间隔设置为10ms或更低，就无法通过LAB 2A many_election test，会大概率在某一阶段无法选出leader。  
解决办法：在resent rpc的循环中也休眠一段时间再发（可能是因为一些服务器disconnected以后，发送rpc的goroutine频繁重试，占用过多cpu时间）
+ 论文中的index从1开始，为了与其统一，我们使用一个{term:0, index:0}占掉log[0]。这在Lab 2D会遇到一些问题，但可以解决。
+ Optimization: commit有两种实现方法：一种是在commitIndex变化的时候由相应的AppendEntries或AppendEntriesReplyHandler向ApplyCh写，另一种是使用独立的goroutine轮询AppliedIndex是否和CommitIndex相等，不相等就commit。如果用第一种实现，在我的实现里偶尔会过不了Lab 2C TestUnreliableChurn2C，会报Fail to reach agreement的错。原因是我在前一种实现里把Lock加在了``ApplyCh<-msg``外面，实际上不应该把锁加在管道处理周围（可能因此需要wait，从而导致长时间选不出leader）
+ Optimization: 快速回退nextIndex的优化是必须做的（要不过不了Lab 2C）。https://thesquareplanet.com/blog/students-guide-to-raft/
+ bug: 如果AppendEntries里的PrevLogIndex小于LastSnapshotIndex，需要考虑怎么合理地返回参数。一个办法是额外加一个参数，然后让leader直接重传之后的所有Log（但是开销太大了）。实际上，这个prevlogindex过小的请求理论上是过时的（因为snapshot里都是已经commit的），所以可以直接返回LastIndex作为conflict index，让leader重新往回倒，这次必然不会倒到snapshot里面。