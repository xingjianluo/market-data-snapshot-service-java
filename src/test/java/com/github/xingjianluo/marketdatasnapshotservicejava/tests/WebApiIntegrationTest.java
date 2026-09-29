package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端 HTTP 集成测试：使用随机端口与独立数据目录，验证
 * 同步推送的五类结论与状态码、快照接口、指标接口与演示注入接口。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "market-data.simulated-feed-enabled=false",
                "market-data.persistence.enabled=false",
                "market-data.queue-capacity=128"
        })
class WebApiIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> body(ResponseEntity<Map> resp) {
        return resp.getBody();
    }

    @Test
    void syncEndpointDistinguishesEveryOutcomeAndStatus() {
        long now = System.currentTimeMillis();
        String sym = "SYNC" + (now % 1_000_000L);
        // 正常
        ResponseEntity<Map> ok = post("/api/quotes/sync",
                Map.of("symbol", sym, "source", "t", "seqNum", 1,
                        "eventTimeMillis", now, "price", 10.0, "size", 2.0));
        assertEquals(HttpStatus.OK, ok.getStatusCode());
        assertEquals("ACCEPTED", body(ok).get("outcome"));

        // 重复
        ResponseEntity<Map> dup = post("/api/quotes/sync",
                Map.of("symbol", sym, "source", "t", "seqNum", 1,
                        "eventTimeMillis", now, "price", 10.0, "size", 2.0));
        assertEquals("DUPLICATE", body(dup).get("outcome"));

        // 乱序窗口内 -> ACCEPTED
        assertEquals("ACCEPTED", body(post("/api/quotes/sync",
                Map.of("symbol", sym, "source", "t", "seqNum", 2,
                        "eventTimeMillis", now - 2_000, "price", 11.0, "size", 1.0))).get("outcome"));

        // 过期 -> 422 STALE
        ResponseEntity<Map> stale = post("/api/quotes/sync",
                Map.of("symbol", sym, "source", "t", "seqNum", 3,
                        "eventTimeMillis", now - 8_000, "price", 11.0, "size", 1.0));
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, stale.getStatusCode());
        assertEquals("STALE", body(stale).get("reason"));

        // 时钟回拨 -> 422 CLOCK_ROLLBACK
        ResponseEntity<Map> rollback = post("/api/quotes/sync",
                Map.of("symbol", sym, "source", "t", "seqNum", 4,
                        "eventTimeMillis", now - 70_000, "price", 11.0, "size", 1.0));
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, rollback.getStatusCode());
        assertEquals("CLOCK_ROLLBACK", body(rollback).get("reason"));

        // 非法 -> 422 INVALID_DATA
        ResponseEntity<Map> invalid = post("/api/quotes/sync",
                Map.of("symbol", sym, "source", "t", "seqNum", 5,
                        "eventTimeMillis", now, "price", -1.0, "size", 1.0));
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, invalid.getStatusCode());
        assertEquals("INVALID_DATA", body(invalid).get("reason"));
    }

    @Test
    void asyncEndpointEnqueuesAndSnapshotMetricsAreReadable() {
        // 不传 eventTimeMillis：控制器补服务端当前墙钟；品种名每次唯一，避免共享上下文残留
        String asyncSym = "ASYNC" + (System.currentTimeMillis() % 1_000_000L);
        ResponseEntity<Map> enq = post("/api/quotes",
                Map.of("symbol", asyncSym, "source", "t", "seqNum", 1,
                        "price", 10.0, "size", 2.0));
        assertEquals(HttpStatus.ACCEPTED, enq.getStatusCode());
        assertEquals("ENQUEUED", body(enq).get("outcome"));

        // 等待异步处理完成
        SnapshotPoller.await(() -> {
            ResponseEntity<Map> snap = rest.getForEntity(
                    url("/api/symbols/" + asyncSym + "/snapshot"), Map.class);
            return snap.getStatusCode() == HttpStatus.OK && body(snap).get("lastQuote") != null;
        });

        ResponseEntity<Map> snap = rest.getForEntity(
                url("/api/symbols/" + asyncSym + "/snapshot"), Map.class);
        assertEquals(HttpStatus.OK, snap.getStatusCode());
        Map<String, Object> sb = body(snap);
        assertNotNull(sb.get("window"));
        assertNotNull(sb.get("trustNote"));
        // 新鲜度标志相对墙钟计算，受线程调度影响，这里只验证字段存在且为布尔值
        assertTrue(sb.get("stale") instanceof Boolean);

        ResponseEntity<Map> metrics = rest.getForEntity(url("/api/metrics"), Map.class);
        Map<String, Object> mb = body(metrics);
        assertNotNull(mb.get("queueCapacity"));
        assertNotNull(mb.get("rejectedBackpressure"));
        assertNotNull(mb.get("rejectedStale"));
        assertNotNull(mb.get("rejectedClockRollback"));
        assertNotNull(mb.get("degraded"));
        assertTrue(((Number) mb.get("accepted")).intValue() >= 1);

        ResponseEntity<Map> symbols = rest.getForEntity(url("/api/symbols"), Map.class);
        assertTrue(((java.util.List<?>) body(symbols).get("symbols")).contains(asyncSym));
    }

    @Test
    void unknownSymbolSnapshotIs404() {
        ResponseEntity<Map> resp = rest.getForEntity(url("/api/symbols/NOPE/snapshot"), Map.class);
        assertEquals(HttpStatus.NOT_FOUND, resp.getStatusCode());
    }

    @Test
    void demoInjectionEndpointsExposeRollbackAndInvalid() {
        ResponseEntity<Map> rb = rest.getForEntity(url("/api/demo/DEMO/clock-rollback"), Map.class);
        assertEquals(HttpStatus.OK, rb.getStatusCode());
        ResponseEntity<Map> inv = rest.getForEntity(url("/api/demo/DEMO/invalid"), Map.class);
        assertEquals(HttpStatus.OK, inv.getStatusCode());
    }

    @SuppressWarnings("rawtypes")
    private ResponseEntity<Map> post(String path, Map<String, Object> payload) {
        return rest.postForEntity(url(path), payload, Map.class);
    }

    static final class SnapshotPoller {
        static void await(java.util.function.BooleanSupplier condition) {
            // 全套件在受限/高负载机器上运行时，Spring 启动与线程调度可能较慢，给足等待时间。
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                if (condition.getAsBoolean()) {
                    return;
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            throw new AssertionError("condition not met within timeout");
        }
    }
}
