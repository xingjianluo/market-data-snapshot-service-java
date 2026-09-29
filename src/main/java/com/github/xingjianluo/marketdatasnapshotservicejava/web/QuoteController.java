package com.github.xingjianluo.marketdatasnapshotservicejava.web;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 行情写入与查询接口。
 */
@RestController
public class QuoteController {

    private final IngestService ingestService;

    public QuoteController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping("/api/quotes")
    public ResponseEntity<IngestResult> publish(@RequestBody QuoteRequest request) {
        IngestResult result = ingestService.submit(request);
        return ResponseEntity.status(result.accepted() ? 202 : 422).body(result);
    }

    @GetMapping("/api/symbols/{symbol}")
    public ResponseEntity<SymbolView> snapshot(@PathVariable String symbol) {
        return ResponseEntity.of(ingestService.query(symbol));
    }

}
