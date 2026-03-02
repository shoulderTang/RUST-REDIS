# RUST-REDIS

本项目是一个使用 Rust 语言重新实现的 Redis 服务器。它旨在通过 Rust 的安全性和并发特性，提供一个高性能、稳定且易于维护的 Redis 替代方案。项目不仅实现了 Redis 的核心数据结构和命令，还支持集群、哨兵、主从复制等高级功能。

## 功能特性 (Features)

*   **核心数据结构**: 完整支持 String, List, Hash, Set, ZSet, Stream, Geo, Bitmap, HyperLogLog 等 Redis 数据类型。
*   **持久化 (Persistence)**:
    *   **AOF**: 支持 Append Only File 持久化，保证数据不丢失。
    *   **RDB**: 支持内存快照，快速备份和恢复。
*   **高可用性 (High Availability)**:
    *   **主从复制 (Replication)**: 支持一主多从架构，实现读写分离和数据冗余。
    *   **哨兵模式 (Sentinel)**: 提供监控、通知和自动故障转移功能。
    *   **集群模式 (Cluster)**: 完整的分布式集群解决方案，支持自动分片、节点发现和故障转移。
*   **高级功能**:
    *   **Lua 脚本**: 内置 Lua 解释器，支持原子性脚本执行。
    *   **ACL 权限控制**: 支持细粒度的用户认证和权限管理 (Auth, ACL)。
    *   **发布/订阅 (Pub/Sub)**: 支持标准的 Pub/Sub 消息模式。
    *   **事务 (Transactions)**: 支持 MULTI/EXEC/WATCH/DISCARD 事务操作。
    *   **阻塞命令**: 支持 BLPOP, BRPOP, XREAD 等阻塞式读取。
    *   **Stream**: 完整的流数据类型支持 (XADD, XREAD, XGROUP 等)。
*   **高性能**: 基于 Rust 异步运行时 (Tokio) 构建，采用多线程模型，充分利用多核 CPU 优势。

## 快速开始 (Getting Started)

### 1. 环境准备
确保您的系统已安装 Rust 编程语言和 Cargo 包管理器。

### 2. 编译项目
```bash
cd rust-redis
cargo build --release
```
编译完成后，可执行文件将生成在 `target/release/` 目录下。

### 3. 运行服务器

**默认启动:**
```bash
./target/release/server
```

**指定配置文件启动:**
```bash
./target/release/server redis.conf
```

**命令行参数启动:**
```bash
./target/release/server --port 6379 --requirepass "mypassword"
```

### 4. 客户端连接
您可以使用本项目自带的客户端，也可以使用官方的 `redis-cli` 工具。

```bash
# 使用官方 redis-cli
redis-cli -p 6379

# 使用本项目客户端
./target/release/client -h 127.0.0.1 -p 6379
```

## 集群与高级配置

### 启动集群
本项目提供了辅助脚本用于快速启动集群测试环境：
```bash
cd rust-redis
# 启动集群节点
./start-cluster-nodes.sh start
```

```bash
# 创建集群
./create-cluster.sh 
```

```bash
# 停止集群节点
./start-cluster-nodes.sh stop
```

```bash
# 重启集群节点
./start-cluster-nodes.sh restart
```

在配置文件中开启集群支持：
```conf
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
```

## 单元测试 (Unit Tests)

本项目提供了全面的单元测试，覆盖了所有核心功能和命令。测试代码位于 `rust-redis/src/tests/` 目录下，按照功能模块进行分类。

### 运行所有测试

```bash
cd rust-redis
cargo test
```

### 运行指定测试

```bash
# 运行特定模块的测试
cargo test --test test_cluster_cmd     # 集群相关测试
cargo test --test test_replication     # 主从复制测试
cargo test --test test_sentinel_logic  # 哨兵模式测试

# 运行特定数据类型的测试
cargo test --test string              # 字符串命令测试
cargo test --test hash                # 哈希命令测试
cargo test --test list                # 列表命令测试
cargo test --test set                 # 集合命令测试
cargo test --test zset                # 有序集合测试
```

### 测试覆盖范围

- **核心命令**: 所有 Redis 数据类型的基本操作命令
- **持久化**: AOF 和 RDB 的保存、加载、恢复功能
- **高可用性**: 主从复制、哨兵模式、集群模式的功能验证
- **高级功能**: Lua 脚本、事务、发布订阅、流数据等
- **性能测试**: 关键命令的性能基准测试

## 更新日志 (Changelog)

2026.1.15 实现了resp模块的解析和序列化，以及db模块的简单实现。

2026.1.16 1.实现简单的日志和配置模块 2.增加shutdown命令，用于关闭服务器。

2026.1.19 性能优化，get,set命令redis-benchmark测试达到redis6.2.5的90%以上; 

2026.1.20 1.增加key过期清理功能。2.增加expire,ttl,dbsize命令。3.增加命令相关的单元测试代码。

2026.2.22 1.增加list类型的命令实现。2.增加hash类型的命令实现。3.增加set/zset类型的命令实现。4.代码结构优化，增加模块之间的解耦。

2026.1.23 1.增加aof/rdb持久化功能。2.set命令增加过期时间参数。3.增加del,mset,mget,keys命令。4.增加相关的单元测试代码。

2026.1.26 1.增加lua脚本功能。2.实现Stream类型的命令。3.增加geo类型的命令实现。 4.增加loglog类型的命令实现。 5.增加select命令，用于切换数据库。

2026.1.27 1.增加auth命令，用于认证客户端连接。 2.增加acl功能，用于权限管理。3.list,zset阻塞性命令实现。 4.增加stream类型的阻塞性命令实现。 5.增加事务相关命令实现。 6.增加pubsub相关命令实现。

2026.1.28 1.增加 EXISTS, TYPE, FLUSHDB, FLUSHALL , RENAME, RENAMENX, PERSIST 命令实现。 2.增加expire,pexpire,expireat,pexpireat,ttl,pttl命令实现。 3.增加SCAN，HSCAN，SSCAN，ZSCAN命令实现。 

2026.1.29 1.tests下代码结构优化，避免重复修改每个用例创建server_context和connection_context的代码。2.增加info,client,monitor,slowlog相关命令的实现。 3.修复aof阻塞写命令需要重写为非阻塞命令的问题。

2026.1.30 1.string,list,hash,set,zset 类型命令补全. 2.run_cmd 函数优化，避免重复创建resp_args。 3.实现 XPENDING, XCLAIM, XAUTOCLAIM 命令。 4.实现 SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS, BITFIELD 命令。

2026.1.31 1.实现 WATCH, UNWATCH 命令。 2.实现 GEOSEARCH, GEOSEARCHSTORE 命令。 3.实现 MOVE, SWAPDB 命令。

2026.2.1 补全单机版缺少的绝大部分命令实现。

2026.2.22 1.实现主从复制功能。2.增加info replication子项，用于查看主从复制相关信息。3.实现waitmaster命令，用于等待主节点同步完成。

2026.2.24 实现哨兵模式功能。

2026.2.26 实现集群模式功能。1.实现cluster节点发现功能。2.实现cluster节点通信功能。3.实现cluster节点故障检测功能。4.实现cluster节点故障转移功能。
