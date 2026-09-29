package com.github.xingjianluo.marketdatasnapshotservicejava.web;

import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 可观察性与管理：积压、拒绝计数、新鲜度、降级状态、手动检查点。
 */
@RestController
public class AdminController {

    private final IngestService ingestService;

    public AdminController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @GetMapping("/admin/metrics")
    public MetricsSnapshot metrics() {
        return ingestService.metrics();
    }

    @GetMapping("/admin/health")
    public String health() {
        return "UP";
    }

    @PostMapping("/admin/checkpoint")
    public String checkpoint() {
        ingestService.checkpointNow();
        return "OK";
    }
}
