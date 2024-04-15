## Problems
1. 如果把ticker的唤醒间隔设置为10ms或更低，就无法通过LAB 2A many_election test，会大概率在某一阶段无法选出leader。  
解决办法：在resent rpc的循环中也休眠一段时间再发（可能是因为一些服务器disconnected以后，发送rpc的goroutine频繁重试，占用过多cpu时间）