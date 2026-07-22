package com.hmdp.controller;

import com.hmdp.analytics.AnalyticsQueryService;
import com.hmdp.dto.Result;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
@RequestMapping("/analytics")
@ConditionalOnProperty(name = "hmdp.analytics.query.enabled", havingValue = "true")
public class AnalyticsController {

    private final AnalyticsQueryService analyticsQueryService;

    public AnalyticsController(AnalyticsQueryService analyticsQueryService) {
        this.analyticsQueryService = analyticsQueryService;
    }

    @GetMapping("/dashboard")
    public Result dashboard(@RequestParam(value = "date", required = false)
                            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        return Result.ok(analyticsQueryService.dashboard(date == null ? LocalDate.now() : date));
    }

    @GetMapping("/shop/trend")
    public Result shopTrend(@RequestParam("shopId") Long shopId,
                            @RequestParam("from") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
                            @RequestParam("to") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        return Result.ok(analyticsQueryService.shopTrend(shopId, from, to));
    }
}
