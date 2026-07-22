package com.hmdp.analytics;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
@ConditionalOnProperty(name = "hmdp.analytics.query.enabled", havingValue = "true")
public class AnalyticsQueryConfig {

    @Bean("analyticsJdbcTemplate")
    public JdbcTemplate analyticsJdbcTemplate(@Value("${hmdp.analytics.query.url}") String url,
                                               @Value("${hmdp.analytics.query.username}") String username,
                                               @Value("${hmdp.analytics.query.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return new JdbcTemplate(dataSource);
    }
}
