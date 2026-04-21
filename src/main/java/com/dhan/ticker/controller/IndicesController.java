package com.dhan.ticker.controller;

import com.dhan.ticker.model.IndexInstrument;
import com.dhan.ticker.service.MasterDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/indices")
@Tag(name = "Indices", description = "Master data - available INDEX instruments")
public class IndicesController {

    private final MasterDataService masterDataService;

    public IndicesController(MasterDataService masterDataService) {
        this.masterDataService = masterDataService;
    }

    @GetMapping
    @Operation(summary = "List all available indices",
            description = "Returns list of INDEX instruments from Dhan master data with symbol, securityId, and exchange segment")
    public ResponseEntity<List<IndexInstrument>> getAllIndices() {
        List<IndexInstrument> indices = masterDataService.getAllIndices();
        return ResponseEntity.ok(indices);
    }
}
