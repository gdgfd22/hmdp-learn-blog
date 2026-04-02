package com.hmdp.Interceptor;

import com.hmdp.utils.TraceIdConstants;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.UUID;

@Component
public class TraceIdInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String traceId = request.getHeader(TraceIdConstants.TRACE_ID_HEADER);
        if (traceId == null || traceId.trim().isEmpty()) {
            traceId = UUID.randomUUID().toString().replace("-", "");
        } else {
            traceId = traceId.trim();
        }
        request.setAttribute(TraceIdConstants.TRACE_ID, traceId);
        response.setHeader(TraceIdConstants.TRACE_ID_HEADER, traceId);
        MDC.put(TraceIdConstants.TRACE_ID, traceId);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        MDC.remove(TraceIdConstants.TRACE_ID);
    }
}
