package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 启用内置模拟源跑真实 Web 端口：验证进程内源喂数据，
 * 指标中能观察到 ACCEPTED 以及源主动制造的 DUPLICATE/OUT_OF_ORDER，
 * 查询持续可用且最新数据非 stale。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "market.service.state-dir=target/test-state-feed-",
        "market.service.feed-enabled=true",
        "market.service.feed-interval-millis=2",
        "market.service.feed-symbols=BTCUSDT",
        "market.service.checkpoint-interval-messages=1000000",
        "market.service.checkpoint-interval-millis=1000000"
})
class SimulatedFeedIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    private TestRestTemplate rest;

    @Test
    void feedProducesAcceptedDuplicateAndOutOfOrderTraffic() throws Exception {
        long deadline = System.currentTimeMillis() + 8_000;
        MetricsSnapshot m = null;
        while (System.currentTimeMillis() < deadline) {
            m = rest.getForObject(url("/admin/metrics"), MetricsSnapshot.class);
            if (m.accepted() > 20 && m.duplicates() > 0 && m.outOfOrderAccepted() > 0) {
                break;
            }
            Thread.sleep(50);
        }
        assertTrue(m != null);
        assertTrue(m.accepted() > 20, "accepted traffic from feed: " + m.accepted());
        assertTrue(m.duplicates() > 0, "feed deliberately resends -> duplicates");
        assertTrue(m.outOfOrderAccepted() > 0, "feed deliberately reorders -> out-of-order");

        SymbolView view = rest.getForObject(url("/api/symbols/BTCUSDT"), SymbolView.class);
        assertTrue(view != null);
        assertTrue(!view.stale(), "freshly fed data should be trusted");
        assertTrue(view.dataAgeMillis() >= 0);
        assertTrue(!view.windows().isEmpty(), "window aggregation should be populated");
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }
}
