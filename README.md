# RUST-REDIS
Rewrite the redis in Rust language and in Rust way.

2026.1.15 开始重写 redis 项目，目标是用 Rust 语言重写 redis 项目，并且用 Rust 的方式来实现。
今天实现了 resp 模块的解析和序列化，以及 db 模块的简单实现。

2026.1.16 1.命令处理改为单线程执行 2.实现简单的日志和配置模块 3.增加shutdown命令，用于关闭服务器。

2026.1.19 性能优化，get,set命令redis-benchmark测试达到redis6.2.5的90%以上; 

2026.1.20 1.增加key过期清理功能。2.增加expire,ttl,dbsize命令。3.增加命令相关的单元测试代码。

2026.2.22 1.增加list类型的命令实现。2.增加hash类型的命令实现。3.增加set/zset类型的命令实现。4.代码结构优化，增加模块之间的解耦。

2026.1.23 1.增加aof/rdb持久化功能。2.set命令增加过期时间参数。3.增加del,mset,mget,keys命令。4.增加相关的单元测试代码。

2026.1.26 1.增加lua脚本功能。2.实现Stream类型的命令。3.增加geo类型的命令实现。 4.增加loglog类型的命令实现。 5.增加select命令，用于切换数据库。

2026.1.27 1.增加auth命令，用于认证客户端连接。 2.增加acl功能，用于权限管理。3.list,zset阻塞性命令实现。 4.增加stream类型的阻塞性命令实现。 5.增加事务相关命令实现。 6.增加pubsub相关命令实现。
