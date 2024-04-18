# My solution for MIT 65840


## Problems
### Lab 1
+ 记得在每次发送RPC前重置Reply。因为空值在RPC里会被压缩。如果不重置，server往reply里写的0会被压缩掉，client会在收到reply后保持原有值。
### Lab2A
+  如果把ticker的唤醒间隔设置为10ms或更低，就无法通过LAB 2A many_election test，会大概率在某一阶段无法选出leader。  
解决办法：在resent rpc的循环中也休眠一段时间再发（可能是因为一些服务器disconnected以后，发送rpc的goroutine频繁重试，占用过多cpu时间）
+ 论文中的index从1开始，为了与其统一，我们使用一个{term:0, index:0}占掉log[0]。这在Lab 2D会遇到一些问题，但可以解决。

### Lab2D
+ 如果将snapshot lastindex初始化为-1，这会导致相关处理变得复杂,很容易out of range。于是可以考虑初始化为0，然后每次Snapshot()的时候往log[0]写一个{Term:Snapshot lasterm, idx: snapshot lastidx}，然后再往后写。这样的好处是：lastlogindex可以反映snapshot中log的真实长度（即，不包括占位的log[0]）的情况）