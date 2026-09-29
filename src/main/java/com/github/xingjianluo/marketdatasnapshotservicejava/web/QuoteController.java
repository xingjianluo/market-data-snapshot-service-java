package com.github.xingjianluo.marketdatasnapshotservicejava.web;

import com.github.xingjianluo.marketdatasnapshotservicejava.feed.SimulatedMarketDataSource;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.QueueingIngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SnapshotView;
import com.github.xingjianluo.marketdatasnapshotservicejava.web.dto.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.web.dto.ResultResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP 接口：
 * <ul>
 *   <li>POST /api/quotes 提交一条报价（异步入队；背压/非法等返回 429/422 并区分 reason）；</li>
 *   <li>GET  /api/symbols/{symbol}/snapshot 一致时点快照（含新鲜度/可信度说明）；</li>
 *   <li>GET  /api/metrics 积压、各拒绝原因计数、新鲜度相关计数、降级标志；</li>
 *   <li>GET  /api/symbols 当前已知品种；</li>
 *   <li>POST /api/demo/{symbol}/clock-rollback、/invalid 由模拟器注入对照样本。</li>
 * </ul>
 */
@RestController
public class QuoteController {

    private final QueueingIngestService ingestService;
    private final SimulatedMarketDataSource simulatedFeed;
    private final AtomicLong httpSeq = new AtomicLong(1);

    public QuoteController(QueueingIngestService ingestService,
                           SimulatedMarketDataSource simulatedFeed) {
        this.ingestService = ingestService;
        this.simulatedFeed = simulatedFeed;
    }

    @PostMapping("/api/quotes")
    public ResponseEntity<ResultResponse> ingest(@RequestBody QuoteRequest request) {
        long now = System.currentTimeMillis();
        String source = request.source() == null || request.source().isBlank()
                ? "http" : request.source();
        long seq = request.seqNum() == null ? httpSeq.getAndIncrement() : request.seqNum();
        long eventTime = request.eventTimeMillis() == null ? now : request.eventTimeMillis();
        double price = request.price() == null ? Double.NaN : request.price();
        double size = request.size() == null ? Double.NaN : request.size();
        Quote quote = new Quote(request.symbol(), source, seq, eventTime, price, size, now);

        IngestionResult result = ingestService.submit(quote);
        if (result == null) {
            // 已入队，最终结论异步产生；队列级背压不涉及内容判定。
            return ResponseEntity.accepted()
                    .body(new ResultResponse("ENQUEUED", "NONE",
                            "quote accepted into bounded queue, processing is asynchronous"));
        }
        HttpStatus status = result.reason() == RejectReason.BACKPRESSURE
                ? HttpStatus.TOO_MANY_REQUESTS : HttpStatus.UNPROCESSABLE_ENTITY;
        return ResponseEntity.status(status)
                .body(new ResultResponse(result.outcome().name(), result.reason().name(),
                        result.detail()));
    }

    /**
     * 同步处理：在调用线程完成判定并返回真实结论（ACCEPTED/DUPLICATE/REJECTED+reason）。
     * 适合需要立即区分拒绝原因的调用方与本地验证；高吞吐场景使用异步 POST /api/quotes。
     */
    @PostMapping("/api/quotes/sync")
    public ResponseEntity<ResultResponse> ingestSync(@RequestBody QuoteRequest request) {
        long now = System.currentTimeMillis();
        String source = request.source() == null || request.source().isBlank()
                ? "http" : request.source();
        long seq = request.seqNum() == null ? httpSeq.getAndIncrement() : request.seqNum();
        long eventTime = request.eventTimeMillis() == null ? now : request.eventTimeMillis();
        double price = request.price() == null ? Double.NaN : request.price();
        double size = request.size() == null ? Double.NaN : request.size();
        Quote quote = new Quote(request.symbol(), source, seq, eventTime, price, size, now);

        IngestionResult result = ingestService.processInline(quote);
        if (result.outcome() == com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionOutcome.ACCEPTED
                || result.outcome() == com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionOutcome.DUPLICATE) {
            return ResponseEntity.ok(new ResultResponse(result.outcome().name(),
                    result.reason().name(), result.detail()));
        }
        HttpStatus status = result.reason() == RejectReason.BACKPRESSURE
                ? HttpStatus.TOO_MANY_REQUESTS : HttpStatus.UNPROCESSABLE_ENTITY;
        return ResponseEntity.status(status)
                .body(new ResultResponse(result.outcome().name(), result.reason().name(),
                        result.detail()));
    }

    @GetMapping("/api/symbols/{symbol}/snapshot")
    public ResponseEntity<SnapshotView> snapshot(@PathVariable String symbol) {
        SnapshotView view = ingestService.getSnapshot(symbol);
        if (view == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        return ResponseEntity.ok(view);
    }

    @GetMapping("/api/metrics")
    public MetricsSnapshot metrics() {
        return ingestService.getMetrics();
    }

    @GetMapping("/api/symbols")
    public Map<String, List<String>> symbols() {
        return Map.of("symbols", ingestService.getSymbols());
    }

    @GetMapping("/api/demo/{symbol}/clock-rollback")
    public Quote demoClockRollback(@PathVariable String symbol) {
        return simulatedFeed.injectClockRollback(symbol);
    }

    @GetMapping("/api/demo/{symbol}/invalid")
    public Quote demoInvalid(@PathVariable String symbol) {
        return simulatedFeed.injectInvalid(symbol);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ResultResponse> handle(Exception e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ResultResponse("REJECTED", "INVALID_DATA",
                        "malformed request: " + e.getMessage()));
    }
}
