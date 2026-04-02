package com.hmdp.aspect;

import com.hmdp.dto.Result;
import com.hmdp.utils.TraceIdConstants;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@Aspect
@Component
public class RequestLogAspect {

    @Around("execution(public * com.hmdp.controller..*.*(..))")
    public Object logRequestCost(ProceedingJoinPoint joinPoint) throws Throwable {
        long startTime = System.currentTimeMillis();
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        HttpServletRequest request = getCurrentRequest();
        String traceId = MDC.get(TraceIdConstants.TRACE_ID);
        String requestMethod = request == null ? "N/A" : request.getMethod();
        String requestUri = request == null ? "N/A" : request.getRequestURI();
        String methodName = signature.getDeclaringType().getSimpleName() + "." + signature.getName();

        try {
            Object result = joinPoint.proceed();
            long cost = System.currentTimeMillis() - startTime;
            if (result instanceof Result) {
                Result response = (Result) result;
                log.info("request finished traceId={}, method={}, uri={}, handler={}, success={}, cost={}ms",
                        traceId, requestMethod, requestUri, methodName, response.getSuccess(), cost);
            } else {
                log.info("request finished traceId={}, method={}, uri={}, handler={}, cost={}ms",
                        traceId, requestMethod, requestUri, methodName, cost);
            }
            return result;
        } catch (Throwable ex) {
            long cost = System.currentTimeMillis() - startTime;
            log.error("request failed traceId={}, method={}, uri={}, handler={}, cost={}ms",
                    traceId, requestMethod, requestUri, methodName, cost, ex);
            throw ex;
        }
    }

    private HttpServletRequest getCurrentRequest() {
        RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
        if (!(attributes instanceof ServletRequestAttributes)) {
            return null;
        }
        return ((ServletRequestAttributes) attributes).getRequest();
    }
}
