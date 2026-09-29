package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 端到端：写入/查询/指标，以及不同拒绝原因在响应体中可区分。
 */
@SpringBootTest(properties = {
        "market.service.state-dir=target/test-state-http-",
        "market.service.feed-enabled=false",
        "market.service.checkpoint-interval-messages=1000000",
        "market.service.checkpoint-interval-millis=1000000"
})
@AutoConfigureMockMvc
class HttpApiIntegrationTest {

    @Autowired
    private MockMvc mvc;

    private long now() {
        return System.currentTimeMillis();
    }

    @Test
    void publishQueryAndMetricsWorkEndToEnd() throws Exception {
        long t = now();
        String body = "{\"eventId\":\"http-1\",\"symbol\":\"BTCUSDT\",\"eventTime\":"
                + t + ",\"price\":50000,\"size\":3}";
        mvc.perform(post("/api/quotes").contentType("application/json").content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.reason").value("ACCEPTED"))
                .andExpect(jsonPath("$.quote.eventId").value("http-1"));

        mvc.perform(get("/api/symbols/BTCUSDT"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.symbol").value("BTCUSDT"))
                .andExpect(jsonPath("$.latest.eventId").value("http-1"))
                .andExpect(jsonPath("$.latest.price").value(50000));

        mvc.perform(get("/admin/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.received").exists())
                .andExpect(jsonPath("$.queueCapacity").exists())
                .andExpect(jsonPath("$.degraded").exists());
    }

    @Test
    void duplicateAndInvalidAndUnknownSymbolAreDistinguishable() throws Exception {
        long t = now();
        String body = "{\"eventId\":\"http-dup\",\"symbol\":\"ETHUSDT\",\"eventTime\":"
                + t + ",\"price\":1800,\"size\":1}";
        mvc.perform(post("/api/quotes").contentType("application/json").content(body))
                .andExpect(status().isAccepted());
        mvc.perform(post("/api/quotes").contentType("application/json").content(body))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("DUPLICATE"));

        String invalid = "{\"eventId\":\"x\",\"symbol\":\"ETHUSDT\"}";
        mvc.perform(post("/api/quotes").contentType("application/json").content(invalid))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("INVALID_DATA"));

        String badJson = "{not-json";
        mvc.perform(post("/api/quotes").contentType("application/json").content(badJson))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("INVALID_DATA"));

        mvc.perform(get("/api/symbols/UNKNOWN_SYMBOL"))
                .andExpect(status().isNotFound());

        mvc.perform(get("/admin/health")).andExpect(status().isOk());
    }
}
