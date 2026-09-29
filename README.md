# 本地行情快照服务（Market Data Snapshot Service）

一个**完全本地、无外部依赖**的行情快照服务：用进程内模拟行情源喂数据，演示并保证
对**乱序、重复、延迟（过期）、时钟回拨**数据的处理结果可预期、可解释、可观察，
并支持**高并发查询**、**本地状态恢复**与**写入背压**。

- 单一 Spring Boot 可执行 jar，`mvn` 离线即可构建/测试/运行
- 不依赖任何外部服务、消息队列、数据库、真实凭据或付费资源
- 持久化只用本地文件（WAL + checkpoint，JSON）

---

## 1. 快速开始

### 1.1 构建与测试

```bash
mvn -o package            # 离线构建（首次需本机已有依赖缓存）
mvn -o test               # 运行全部 18 个测试
```

### 1.2 启动（带内置模拟行情源）

```bash
java -jar target/market-data-snapshot-service-java-1.0-SNAPSHOT.jar \
  --market.service.feed-enabled=true \
  --market.service.feed-interval-millis=10
```

启动后模拟源会对 `BTCUSDT,ETHUSDT` 持续推送随机游走行情，并**周期性制造**：

- 每 11 条重发上一条（相同 `eventId`）→ 观察 `DUPLICATE`
- 每 13 条发一条 `eventTime` 回拨 2s 的消息 → 观察 `OUT_OF_ORDER`

### 1.3 接口调用

```bash
# 写入一条行情
curl -X POST localhost:8080/api/quotes -H 'Content-Type: application/json' -d '{
  "eventId":"evt-1","symbol":"BTCUSDT","eventTime":1790000000000,
  "price":50000,"size":3}'

# 查询一致时点快照（最新价 + 各窗口聚合）
curl localhost:8080/api/symbols/BTCUSDT

# 运行指标：积压、各拒绝原因计数、新鲜度、降级
curl localhost:8080/admin/metrics

# 手动检查点 / 健康检查
curl -X POST localhost:8080/admin/checkpoint
curl localhost:8080/admin/health
```

### 1.4 本地复现背压

```bash
java -jar target/*.jar \
  --market.service.queue-capacity=8 \
  --market.service.enqueue-timeout-millis=5 \
  --market.service.process-delay-millis=2      # 压低处理能力，确定性制造过载
# 另一个终端并发突发写入：
seq 1 2000 | xargs -P16 -I{} curl -s -o /dev/null -X POST localhost:8080/api/quotes \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"bp-{}","symbol":"BP","eventTime":1790000000000,"price":100,"size":1}'
curl localhost:8080/admin/metrics   # backpressureRejected > 0，queueDepth 不超过 8
```

### 1.5 验证重启恢复

服务正常停止（SIGTERM）会做最终检查点并 fsync WAL。重新用**同一个**
`--market.service.state-dir` 启动即可自动恢复；重发历史 `eventId` 仍返回
`DUPLICATE`，窗口计数不翻倍。

---

## 2. 数据模型

所有价格/数量使用 `long` 整数档位，避免浮点导致的聚合不确定性。

| 类型 | 含义 |
| --- | --- |
| `QuoteRequest` | 客户端原始推送：`eventId, symbol, eventTime(epoch ms), price, size`，接收时间由服务端加盖 |
| `Quote` | 已接收的不可变行情（含服务端处理后的字段） |
| `WindowStats` | 一个滚动窗口（左闭右开 `[windowStart, windowEnd)`）的聚合：`count, open, high, low, close, totalVolume` |
| `SymbolView` | 某品种在**同一临界区**内取出的一致快照：`latest + stale + dataAgeMillis + windows[]` |
| `IngestResult` | 写入判定：`accepted, reason(枚举), detail(人类可读依据), serverReceiveTime, quote` |
| `MetricsSnapshot` | 吞吐/各拒绝计数/队列积压/最老排队年龄/各品种数据年龄/检查点计数/降级标志 |

窗口大小、保留窗口数可配（默认 1s 窗口、每品种保留 60 个）。

### 窗口聚合规则（与到达顺序无关，乱序归位结果确定）

- `open`  = 窗口内 **eventTime 最小**（相同 eventTime 取 eventId 字典序最小）的价格
- `close` = 窗口内 **eventTime 最大**（相同 eventTime 取 eventId 字典序最大）的价格
- `high`/`low` = 窗口内价格极值
- `count`/`totalVolume` = 条数 / 成交量之和

因此先收到 100200、后收到 100100，窗口的 open/close 仍按时间正确归位。

---

## 3. 去重、排序与拒绝原因

处理在**单个工作线程**上完成，判定顺序固定（见下表）。任何不被计入的消息都会
**显式返回原因并打印日志**，绝不静默丢弃。

| 顺序 | reason | 是否计入 | 判定依据 |
| --- | --- | --- | --- |
| 提交线程 | `INVALID_DATA` | 否 | eventId/symbol/eventTime/price/size 缺失或非正数；JSON 无法解析 |
| 1 | `CLOCK_ROLLBACK` | 否 | `serverReceiveTime - eventTime` 超过 `clock-rollback-age-millis`（过去方向的时钟回拨），或 eventTime 超前服务器同阈值（时钟漂移）。**不占用 eventId**，时钟恢复后可用相同 eventId 重试 |
| 2 | `DUPLICATE` | 否 | `eventId` 此前已计入（去重索引，跨重启持久化） |
| 3 | `EXPIRED` | 否 | `eventTime` 落后该品种最新已接受事件超过 `out-of-order-lag-millis`（业务上过期，延迟过久）；超过 `data-ttl-millis` 时日志明确标注 beyond ttl |
| 4 | `OUT_OF_ORDER` | **是** | 落后最新事件但在 `out-of-order-lag-millis` 内：按 eventTime 归入对应窗口，最新价指针不变 |
| 5 | `ACCEPTED` | **是** | eventTime 推进最新价 |
| 异常 | `PERSISTENCE_FAILURE` | 否 | WAL 落盘失败：**内存不更新**，显式拒绝 |
| 过载 | `BACKPRESSURE` | 否 | 有界队列在 `enqueue-timeout-millis` 内无空位 |
| 关停 | `SERVICE_CLOSED` | 否 | 服务未运行/正在关闭 |

**关键区分：**

- `CLOCK_ROLLBACK` 与 `EXPIRED` 分离：前者是“时间戳相对服务端时钟不可信”（超过 24h
  量级），后者是“相对本品种最新行情这一业务序列太晚”（秒级）。时钟回拨消息不占用
  eventId，恢复时钟后可重试成功。
- `DUPLICATE` 针对的是“同一 eventId 已计入”，因此被 `CLOCK_ROLLBACK`/`EXPIRED`
  拒绝的消息不会污染去重索引。
- `BACKPRESSURE` 与数据质量无关，是容量保护，计数与数据类拒绝分开。

### 去重索引

- 每品种一个有界 `DedupIndex`：按 `eventId` 判重，按 `eventTime` 淘汰最旧条目，
  容量由 `dedup-retained-per-symbol` 控制，防止无界内存增长。
- 索引内容写入检查点，因此**重启后去重仍然生效**；WAL 重放时遇到检查点中已有的
  eventId 直接跳过——重复执行恢复、重复重放同一批历史数据都不会二次累计。

---

## 4. 并发与一致性语义

- **写入**：HTTP 提交线程只做字段校验，然后把请求放入有界队列；**单一 ingest-worker
  线程**串行执行 判定→去重→WAL→内存聚合→检查点，消除了状态更新竞争。
- **读取**：查询直接走内存（`QuoteStore`），**不经过写入队列**，压力下始终可用。
  每个品种一把 `StampedLock`：
  - 写路径在写锁内同时更新“最新价指针”和“窗口聚合”，查询不可能看到只更新了一半的状态；
  - 读路径在读锁/乐观读内一次性拷贝出不可变的 `SymbolView`/`WindowStats`，锁外返回。
- **最新价** = eventTime 最大（相同取 eventId 字典序最大）的行情，与到达顺序解耦。
- **新鲜度/可信度**：`SymbolView.stale` 与 `dataAgeMillis` 显式说明数据是否可信。
  数据年龄超过 `stale-threshold-millis` 或来自“未来”（年龄为负）时 `stale=true`。
  源停更后历史快照仍可查，但会被标记为不可信。

---

## 5. 背压与可观察性

- 队列为 `ArrayBlockingQueue`，容量有界（`queue-capacity`）。满时 `offer` 最多等待
  `enqueue-timeout-millis`，超时返回 `BACKPRESSURE` 并计数——既不无界堆积也不静默丢弃。
- 过载期间 `GET /admin/metrics` 暴露：
  - `queueDepth` / `queueCapacity`：当前积压程度（永远 ≤ 容量）
  - `oldestQueuedAgeMillis`：队列中最老消息等待时长
  - `backpressureRejected`：被背压拒绝的数量
  - `duplicates/invalidData/expired/clockRollback/persistenceFailures`：各原因独立计数
  - `degraded` + `degradedReason`：是否处于降级（过载时为 true，排空恢复后自动解除）
  - `latestEventTimeBySymbol` / `dataAgeMillisBySymbol`：各品种新鲜度
- `process-delay-millis`（默认 0）可人为压低处理速率，用于本地确定性复现背压。

---

## 6. 持久化与恢复语义

状态目录（`market.service.state-dir`）包含：

- `wal.log`：JSON Lines 追加日志。**每条 accepted 行情在更新内存前先 append + flush**，
  保证崩溃不丢已接受数据。
- `checkpoint.json`：一致性检查点（最新价 + 各品种窗口聚合 + 去重索引快照），
  用“临时文件 + 原子 move”写入，崩溃只会读到旧或新完整检查点，不会读到半文件。

恢复流程（在 worker 线程开始消费**之前**执行）：

1. 载入最近检查点，还原最新价、窗口聚合与去重索引；
2. 顺序重放 WAL 中检查点之后落盘的行情，逐条过去重索引；
3. 检查点中已有或 WAL 内重复的条目一律跳过 → **恢复幂等，重复恢复不重复累计**。

检查点触发：每 `checkpoint-interval-messages` 条或每 `checkpoint-interval-millis`
毫秒一次；成功后 WAL 被轮转（已被检查点覆盖的旧日志删除）。正常关闭时也会做一次
最终检查点。`recovery-enabled=false` 可关闭恢复（用于冷启动演示）。

---

## 7. 配置项（前缀 `market.service`）

| 配置键 | 默认值 | 说明 |
| --- | --- | --- |
| `queue-capacity` | 4096 | 写入有界队列容量 |
| `enqueue-timeout-millis` | 50 | 入队等待；超时 → BACKPRESSURE |
| `out-of-order-lag-millis` | 5000 | 乱序容忍窗口，内 → OUT_OF_ORDER 归位 |
| `data-ttl-millis` | 60000 | 数据有效期；超过乱序窗口即判 EXPIRED |
| `clock-rollback-age-millis` | 86400000 | 时钟回拨/漂移阈值（相对服务器接收时间） |
| `window-size-millis` | 1000 | 聚合窗口大小 |
| `retained-windows` | 60 | 每品种保留窗口桶数 |
| `stale-threshold-millis` | 5000 | 超过该年龄标记 stale（不可信） |
| `dedup-retained-per-symbol` | 4096 | 每品种去重索引容量 |
| `state-dir` | market-data-state | WAL + checkpoint 目录 |
| `checkpoint-interval-messages` | 1000 | 每 N 条消息触发检查点 |
| `checkpoint-interval-millis` | 2000 | 检查点最小时间间隔 |
| `recovery-enabled` | true | 启动时是否自动恢复 |
| `feed-enabled` | false | 是否启用内置模拟行情源 |
| `feed-interval-millis` | 20 | 模拟源推送间隔（设很小可加压） |
| `feed-symbols` | BTCUSDT,ETHUSDT | 模拟源品种 |
| `process-delay-millis` | 0 | 单条处理延迟，演示/压测背压用 |

可用 `--market.service.xxx=...`、JVM `-D` 或环境变量覆盖。

---

## 8. HTTP API 摘要

| 方法 & 路径 | 说明 | 成功/失败 |
| --- | --- | --- |
| `POST /api/quotes` | 推送行情 | 计入返回 `202`；拒绝返回 `422`，body 含 reason/detail |
| `GET /api/symbols/{symbol}` | 一致时点快照 | 有数据 `200`；未知品种 `404` |
| `GET /admin/metrics` | 运行指标 | `200` |
| `GET /admin/health` | 健康 | `200` UP |
| `POST /admin/checkpoint` | 立即检查点 | `200` OK |

写入响应 `reason` 取值：
`ACCEPTED, OUT_OF_ORDER, DUPLICATE, INVALID_DATA, EXPIRED, CLOCK_ROLLBACK,
BACKPRESSURE, SERVICE_CLOSED, PERSISTENCE_FAILURE`。

---

## 9. 日志

控制台日志对每条消息打印输入要素与判定依据，例如：

```
INFO  ACCEPT kind=OUT_OF_ORDER eventId=e2 symbol=BTCUSDT eventTime=100100 price=90 size=1 serverTime=... detail=out-of-order within lag 100ms, placed into its eventTime window
INFO  REJECT reason=EXPIRED eventId=old symbol=... eventTime=... detail=eventTime is 60000ms behind latest ..., beyond ttl
WARN  BACKPRESSURE reject: eventId=..., symbol=BP, queueDepth=8/8, detail=offer timeout 5ms
INFO  Recovery duplicate skipped (not re-counted): eventId=..., symbol=...
```

不同拒绝原因在 reason、metrics 计数与日志关键字上都可区分。

---

## 10. 测试覆盖

`src/test/.../tests` 下：

| 测试类 | 覆盖点 |
| --- | --- |
| `OrderingDedupTest` | 乱序归位、open/close 确定性、重复不双计、过期、非法数据、未知品种 |
| `ClockRollbackTest` | 过去/未来时钟回拨与过期区分、回拨不占用 eventId 且可重试 |
| `RestartRecoveryTest` | 仅 WAL 重放、检查点+WAL 合并、重复恢复不翻倍、跨重启去重 |
| `ConcurrencyReadWriteTest` | 8 写者并发 + 持续并发读，无半更新、窗口计数与已接受事件严格一致 |
| `BackpressureTest` | 小队列+多写者过载：有界拒绝、积压/降级指标、压力下查询可用、排空后不丢消息并恢复 |
| `FreshnessTest` | stale 标志随时间翻转、dataAge 指标 |
| `HttpApiIntegrationTest` | REST 写入/查询/指标/404，重复/非法/坏 JSON 原因区分 |
| `SimulatedFeedIntegrationTest` | 内置源端到端：自动产生 accepted/duplicate/out-of-order，数据新鲜 |

```bash
mvn -o test
```

---

## 11. 代码结构

```
model/        数据模型与 reason 枚举
config/       ServiceProperties 配置、Store Bean
clock/        Clock 抽象（SystemClock / 测试用 MutableClock）
store/        QuoteStore：StampedLock 单写多读一致快照
window/       WindowBucket：顺序无关的确定性聚合
dedup/        DedupIndex / 注册表：有界、可持久化的去重
persistence/  WalWriter、CheckpointStore、RecoveryService
ingest/       IngestService（队列/背压/判定/编排）、MetricsCollector
feed/         SimulatedMarketFeed：进程内模拟行情源
web/          QuoteController、AdminController、异常处理
```
