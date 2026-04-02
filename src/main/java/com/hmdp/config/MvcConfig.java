package com.hmdp.config;

import com.hmdp.Interceptor.JwtTokenInterceptor;
import com.hmdp.Interceptor.LoginInterceptor;
import com.hmdp.Interceptor.TraceIdInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Resource
    private LoginInterceptor loginInterceptor;

    @Resource
    private JwtTokenInterceptor jwtTokenInterceptor;

    @Resource
    private TraceIdInterceptor traceIdInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(traceIdInterceptor).order(0);
        registry.addInterceptor(jwtTokenInterceptor).order(1);
        registry.addInterceptor(loginInterceptor)
                .excludePathPatterns(
                        "/user/code",
                        "/user/login",
                        "/user/refresh",
                        "/user/logout"
                )
                .order(2);
    }
}
