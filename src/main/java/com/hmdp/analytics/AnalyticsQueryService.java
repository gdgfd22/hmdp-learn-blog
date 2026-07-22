package com.hmdp.analytics;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@ConditionalOnProperty(name = "hmdp.analytics.query.enabled", havingValue = "true")
public class AnalyticsQueryService {

    private final JdbcTemplate jdbcTemplate;

    public AnalyticsQueryService(@Qualifier("analyticsJdbcTemplate") JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<String, Object> dashboard(LocalDate date) {
        Map<String, Object> result = new LinkedHashMap<>();
        String metricDate = date.toString();
        result.put("overview", jdbcTemplate.queryForList(
                "SELECT * FROM ads_realtime_overview WHERE metric_date = ?", metricDate));
        result.put("shopRank", jdbcTemplate.queryForList(
                "SELECT * FROM ads_shop_rank WHERE metric_date = ? ORDER BY hot_score DESC LIMIT 10", metricDate));
        result.put("blogRank", jdbcTemplate.queryForList(
                "SELECT * FROM ads_blog_rank WHERE metric_date = ? ORDER BY hot_score DESC LIMIT 10", metricDate));
        result.put("voucherFunnel", jdbcTemplate.queryForList(
                "SELECT * FROM ads_voucher_funnel WHERE metric_date = ? ORDER BY exposure_count DESC LIMIT 10", metricDate));
        result.put("quality", jdbcTemplate.queryForList(
                "SELECT * FROM ads_data_quality ORDER BY check_time DESC LIMIT 20"));
        return result;
    }

    public List<Map<String, Object>> shopTrend(Long shopId, LocalDate from, LocalDate to) {
        return jdbcTemplate.queryForList(
                "SELECT * FROM dws_shop_day WHERE shop_id = ? AND metric_date BETWEEN ? AND ? ORDER BY metric_date",
                shopId, from.toString(), to.toString());
    }
}
