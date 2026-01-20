# RUST-REDIS
Rewrite the redis in Rust language and in Rust way.

2026.1.15 开始重写 redis 项目，目标是用 Rust 语言重写 redis 项目，并且用 Rust 的方式来实现。
今天实现了 resp 模块的解析和序列化，以及 db 模块的简单实现。

2026.1.16 1.命令处理改为单线程执行 2.实现简单的日志和配置模块 3.增加shutdown命令，用于关闭服务器。

2026.1.19 性能优化，get,set命令redis-benchmark测试达到redis6.2.5的90%以上; 

2026.1.20 增加命令相关的单元测试代码。

