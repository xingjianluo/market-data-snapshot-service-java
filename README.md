# 本地行情快照服务（market-data-snapshot-service-java）

一个完全本地运行、零外部依赖的行情快照服务：用**进程内模拟行情源**喂数据，
对乱序、重复、延迟和时钟回拨消息给出**可解释、可区分**的处理结果，
支持并发一致查询、窗口幂等聚合、背压保护与进程重启后的状态恢复。

- Java 21+（开发机使用 Java 25 验证）、Spring Boot 3.4、仅用 JDK 自带的并发与文件能力
- 无需数据库、消息中间件、真实凭据或付费资源；状态保存在本地 JSON Lines 事件日志中

---

## 1. 快速开始

```bash
# 构建 + 全部测试（26 个）
mvn clean test

# 启动（默认端口 8080，自动开启模拟行情源，数据落到 ./data/quotes.log）
mvn spring-boot:run
```

启动后模拟器每 200ms 推送 `SIM.A/SIM.B/SIM.C` 三个品种，并周期性注入
**重复、容忍窗口内乱序、过期**样本；观察日志即可看到每条输入与判定依据：

```
... INFO OrderedProcessor - quote accepted phase=online quote=Quote[symbol=SIM.A,...] basis=newer than current max by 82 ms, advances timeline
... INFO OrderedProcessor - quote duplicate phase=online quote=... basis=dedupKey already seen: sim#9
... INFO OrderedProcessor - quote rejected phase=online reason=STALE quote=... basis=eventTime ... is 6,000 ms behind maxEventTime ..., exceeds disorder tolerance 5,000 ms
```

### 常用接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/quotes` | **异步**入队推送。成功返回 `202 ENQUEUED`；队列满返回 `429 BACKPRESSURE` |
| POST | `/api/quotes/sync` | **同步**处理并返回最终结论（ACCEPTED/DUPLICATE/REJECTED+原因），便于验证 |
| GET  | `/api/symbols/{symbol}/snapshot` | 某品种的一致时点快照（含窗口聚合、新鲜度、可信度说明） |
| GET  | `/api/metrics` | 积压程度、各类接受/拒绝计数、恢复计数、降级标志 |
| GET  | `/api/symbols` | 当前已知品种列表 |
| GET  | `/api/demo/{symbol}/clock-rollback` | 触发模拟器注入一条时钟回拨样本 |
| GET  | `/api/demo/{symbol}/invalid` | 触发模拟器注入一条非法数据样本 |

快速手工验证五种结论（同步接口）：

```bash
B=http://localhost:8080
NOW=$(python3 -c 'import time;print(int(time.time()*1000))')
curl -s -X POST $B/api/quotes/sync -H 'Content-Type: application/json' \
  -d "{\"symbol\":\"T\",\"source\":\"t\",\"seqNum\":1,\"eventTimeMillis\":$NOW,\"price\":10,\"size\":1}"
#   接受：{"outcome":"ACCEPTED",...}
curl ... -d "{...,\"seqNum\":1,...}"                  # DUPLICATE
curl ... -d "{...,\"seqNum\":2,\"eventTimeMillis\":$((NOW-2000)),...}"  # 乱序窗口内 → ACCEPTED（归位）
curl ... -d "{...,\"seqNum\":3,\"eventTimeMillis\":$((NOW-8000)),...}"  # 422 STALE
curl ... -d "{...,\"seqNum\":4,\"eventTimeMillis\":$((NOW-70000)),...}" # 422 CLOCK_ROLLBACK
curl ... -d "{...,\"seqNum\":5,\"price\":-1,...}"                        # 422 INVALID_DATA
```

> STALE 与 CLOCK_ROLLBACK 的相对时间以"该品种已接受的最大事件时间"为基准，
> 因此手工验证时请先推一条新消息，再推落后的消息。

---

## 2. 数据模型

### 2.1 输入报价 `Quote`（不可变 record）

| 字段 | 含义 |
|---|---|
| `symbol` | 品种代码，必填非空 |
| `source` | 行情源标识，必填非空 |
| `seqNum` | **行情源内**单调递增的序列号，非负 |
| `eventTimeMillis` | 行情事件时间（epoch ms）。非负，且不得领先墙钟超过 5s |
| `price` / `size` | 价格 / 数量，必须是有限正数 |
| `receivedAtMillis` | 服务端接收时间（由服务端补齐） |

**去重键 = `source + "#" + seqNum`**，按品种独立维护：
同一行情源内同一序列号即视为同一条消息，即使重传时载荷不同也判重；
不同行情源使用相同序列号互不影响。

### 2.2 处理结果 `IngestionResult`

`outcome ∈ {ACCEPTED, DUPLICATE, REJECTED}`，并带可读的 `detail`（判定依据）。
拒绝原因 `RejectReason` 四类，**彼此可区分、均显式返回并计数，绝不静默丢弃**：

| reason | 含义 |
|---|---|
| `INVALID_DATA` | 字段缺失/为负/非有限值、时间戳非法（负值或远超未来） |
| `STALE` | 相对本品种已见最大事件时间落后超过乱序容忍、但未到时钟回拨阈值 |
| `CLOCK_ROLLBACK` | 落后超过回拨阈值，代表来源时钟异常或串源 |
| `BACKPRESSURE` | 有界队列已满，通道级显式拒绝 |

### 2.3 快照 `SnapshotView`

某品种的不可变一致视图：`lastQuote`（事件时间最大的报价）、
`lastUpdateEventTimeMillis`、窗口聚合 `window`、
`stale`/`stalenessMillis`（**相对墙钟**的数据新鲜度）、
`degraded`（近期发生过背压）、`trustNote`（人读的可信度结论）。

### 2.4 窗口聚合 `WindowAggregate`

固定长度的**尾随窗口** `(maxEventTime - windowLength, maxEventTime]`（左开右闭），
输出 tick 数、OHLC、成交量、VWAP。

---

## 3. 去重与排序规则

单条消息按以下顺序判定（全部发生在该品种的锁内，复合原子）：

1. **非法数据校验** → `INVALID_DATA`。非法消息不改变任何状态，也不写入恢复日志。
2. **去重判定** → `(source, seqNum)` 已见过则 `DUPLICATE`，不重复计入聚合/计数。
3. **时钟回拨判定** → 令 `gap = 本品种maxEventTime - msg.eventTime`；
   `gap > clockRollbackThresholdMillis` 判 `CLOCK_ROLLBACK`。
4. **过期判定** → `gap > disorderToleranceMillis`（且未超过回拨阈值）判 `STALE`。
5. **接受归位** → 首条消息、时间更新的消息、以及
   `0 ≤ gap ≤ disorderToleranceMillis` 的轻微乱序消息全部接受；
   最新报价与窗口均**按事件时间归位**，乱序消息不会覆盖更新时间更大的最新价。

两个"过期"概念请注意区分：

- **拒绝原因 STALE**：消息事件时间相对本品种已见序列太旧（序列视角）；
- **快照字段 `stale`**：最新事件时间相对**墙钟**太久没更新（新鲜度视角，
  由 `freshnessToleranceMillis` 控制）。

**时钟回拨 vs 一般乱序**的区分是有意的：落后量在容忍窗口内视为网络抖动，
归位接受；落后很多但不到回拨阈值（默认 5s~60s）视为无法归位的过期；
落后超过阈值（默认 60s）视为对端时钟/来源异常，单独标记 `CLOCK_ROLLBACK`，
便于运维与上游时钟问题区分处理。

### 聚合幂等性

窗口内以去重键保存报价（`Map<dedupKey, Quote>`）：

- 重复消息/重启后重放同一条消息，命中同一键 → 不会二次计入；
- OHLC/VWAP 由集合内容计算，与消息到达顺序无关（`open/close` 取窗口内
  事件时间最早/最晚，而非到达最早/最晚）；
- 窗口随最大事件时间单调前移，越界报价按确定规则剔除。

因此同一份历史无论按什么顺序、重放多少次，最终聚合结果一致。

---

## 4. 并发与恢复语义

### 4.1 写入通道与背压

```
HTTP/模拟器 ──offer(非阻塞)──▶ 有界 LinkedBlockingQueue ──▶ 单消费者线程 ──▶ OrderedProcessor
```

- 队列容量固定（`market-data.queue-capacity`）。**满了立即返回 `BACKPRESSURE`（HTTP 429）**，
  调用方应重试；既不无界堆积，也不在入队后悄悄丢消息（已入队消息保证被处理）。
- 单一消费者线程按 FIFO 处理；模拟器与测试用的同步入口 `processInline`
  与消费者共用同一把 `writeLock`，全系统对状态而言是**单写者**。
- 发生背压拒绝后的 `degradedHoldMillis`（默认 2s）内，服务标记为 `degraded`，
  快照 `trustNote` 会提示数据可能滞后；压力解除后自动恢复。

### 4.2 并发读写一致性

- 每个品种有独立的 `SymbolState`，"判重 → 乱序/回拨判定 → 更新最新价 → 更新窗口"
  在该对象的锁内一次性完成；
- 快照查询在同一把锁内把状态**复制为不可变视图**后立即释放锁，
  因此读者要么看到更新前、要么看到更新后的完整时点，不会读到半更新，
  也不会长时间阻塞写者（copy-on-read）。
- 品种注册表用 `ConcurrentHashMap.computeIfAbsent` 保证每品种状态单例。

### 4.3 持久化与重启恢复

- **只有被接受的消息**追加写入本地事件日志（`dataDir/prefix.log`，JSON Lines）。
  重复、过期、回拨、非法、背压拒绝的消息都不落盘。
- 日志按大小滚动到 `prefix.log.1`（保留最近一段）；可配置每条 `fsync`。
- 启动时在消费者线程启动前重放日志（`.1` 旧段 → 当前段，段内按行序），
  走与在线完全相同的处理器路径。由于处理器按去重键幂等：
  - 同一日志被重放多次（例如重复"恢复"）计数不翻倍；
  - 恢复后重发历史消息仍判 `DUPLICATE`；
  - 损坏的日志行跳过并告警，不中断其余恢复。
- 恢复计数单独统计为 `recovered`，不混入在线吞吐指标。

> 持久性边界：默认 `log-fsync=false` 以换取吞吐，极端崩溃（OS 崩溃/断电）
> 可能丢失最后若干条已接受记录；正常 `SIGTERM` 优雅关闭不丢已入队数据
> （关闭时会排空队列再关闭日志）。需要更强保证可设 `market-data.log-fsync=true`。

---

## 5. 配置项（`application.properties`，前缀 `market-data`）

| key | 默认值 | 说明 |
|---|---|---|
| `queue-capacity` | 2048 | 入站有界队列容量；满即背压拒绝 |
| `disorder-tolerance-millis` | 5000 | 乱序容忍窗口，窗口内落后消息归位接受 |
| `clock-rollback-threshold-millis` | 60000 | 超过该落后量判时钟回拨 |
| `freshness-tolerance-millis` | 3000 | 快照相对墙钟的新鲜度容忍 |
| `window-length-millis` | 60000 | 尾随聚合窗口长度 |
| `dedup-cache-capacity` | 100000 | 每品种 LRU 去重缓存容量 |
| `persistence.enabled` | true | 是否启用事件日志与恢复 |
| `data-dir` | `./data` | 事件日志目录 |
| `log-file-prefix` | `quotes` | 日志文件名前缀 |
| `log-max-file-bytes` | 67108864 | 单段日志大小上限，超过滚动到 `.1` |
| `log-fsync` | false | 每条记录是否 fsync |
| `simulated-feed-enabled` | true | 是否启动进程内模拟行情源 |
| `simulated-feed-interval-millis` | 200 | 模拟器推送节拍 |

> 属性名为标准 Spring Boot kebab-case，例如开关是
> `market-data.simulated-feed-enabled`（不是 `simulated-feed.enabled`）。

命令行覆盖示例：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=\
"--market-data.queue-capacity=64,--market-data.data-dir=/tmp/md,--market-data.simulated-feed-enabled=false"
```

---

## 6. 观测性

`GET /api/metrics` 返回（见 `MetricsSnapshot`）：

- `received / accepted / duplicate / rejected`
- 各拒绝原因分项：`rejectedInvalid / rejectedStale / rejectedClockRollback / rejectedBackpressure`
- `queueCapacity / queueSize`：**积压程度**
- `recovered`：本次启动恢复的接受条数
- `degraded`：是否处于背压降级窗口
- `uptimeMillis`、`acceptedBySymbol`

快照中的 `trustNote` 给出当前数据是否可信的结论：
`OK` / `STALE` / `DEGRADED` / `UNTRUSTED`（降级且陈旧）/ `NO_DATA`。

所有判定都以 INFO 日志打印**输入报价与判定依据**（字段值、落后量、阈值），
便于在日志中区分各类拒绝原因。

---

## 7. 验证方法

```bash
mvn clean test
```

| 测试类 | 覆盖点 |
|---|---|
| `OrderedProcessorTest`（10） | 首条、重复（跨源同号不判重）、窗口内乱序归位、STALE、CLOCK_ROLLBACK、非法数据、未来时间、新鲜度翻转、恢复幂等 |
| `AppendEventLogTest`（4） | 追加/重放顺序、重复读取稳定、按大小滚动后旧→新重放、损坏行跳过 |
| `ConcurrencyConsistencyTest`（2） | 8 写线程×500 + 4 并发快照读者：计数精确、快照始终内部自洽；并发重复只计一次 |
| `BackpressureTest`（2） | 队列满显式 429/计数可观察、已入队不丢、压力下查询可响应、降级标志自动恢复 |
| `RecoveryRestartTest`（2） | 两次重启后状态恢复、重复恢复不翻倍、历史重放判重、拒绝消息不落盘 |
| `PerSymbolIsolationTest`（1） | 回拨/乱序/去重按品种独立 |
| `WebApiIntegrationTest`（4） | 五类结论与 HTTP 状态码、快照/指标/品种接口、演示注入接口 |
| `ApplicationTests`（1） | Spring 上下文加载 |

端到端手工验证见第 1 节；重启恢复的手工步骤：

```bash
mvn spring-boot:run                       # 推若干条报价，Ctrl-C / kill
mvn spring-boot:run                       # 同目录重启
curl -s localhost:8080/api/metrics        # "recovered" 等于上次接受条数
curl -s localhost:8080/api/symbols/SIM.A/snapshot   # 聚合恢复，且不翻倍
```

---

## 8. 代码结构

```
model/      Quote / IngestionResult / RejectReason / SnapshotView / WindowAggregate / MetricsSnapshot
store/      DedupCache(LRU) / SymbolState(品种锁+尾随窗口) / InMemoryStateStore / AppendEventLog(JSON Lines WAL)
engine/     OrderedProcessor(校验/判重/乱序/回拨/快照) / ServiceMetrics
ingest/     QueueingIngestService(有界队列+单消费者+背压+恢复+优雅关闭)
feed/       SimulatedMarketDataSource(进程内行情源与异常样本注入)
web/        QuoteController + dto
config/     ServiceProperties + ServiceConfiguration
```

## 9. 已知边界与权衡

- 去重缓存为有界 LRU：超出容量后极旧键被淘汰，理论上极旧序列号的"重传"
  可能被当作新消息。容量可配、可观测；本地场景默认 10 万/品种足够。
- 事件日志只保留最近一个滚动段，重放只承诺恢复保留段内的接受历史。
- 背压策略是**快速失败 + 显式原因**（而非无限阻塞调用方），
  由调用方决定重试/降速；这保证压力下查询接口始终可用。
