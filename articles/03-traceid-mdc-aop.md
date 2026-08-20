# 请求全链路追踪：TraceID + MDC + AOP 接口耗时埋点

> 摘要：一次请求从进入系统到返回结果，要经过拦截器、Controller、Service、DAO 多层调用，在并发场景下日志很容易交叉混在一起，出了问题很难把同一次请求的日志串起来。本文介绍项目里的做法：用 TraceIdInterceptor 生成或透传 TraceID，放进 SLF4J 的 MDC 上下文，通过日志模板自动输出，再用 AOP 环绕通知对 Controller 层统一埋点，记录请求方法、路径、处理方法和耗时。需要说明的是，目前实现的是"请求级"链路日志，没有 Span、没有跨服务采样，与 OpenTelemetry 这类分布式追踪还有明显差距，且全部验证都在本地单机/容器环境完成。

## 一、为什么要这样做（业务背景与痛点）

服务端开发尤其是多线程、Web 请求、微服务场景下，日志很容易变成这个样子：

```
用户A的请求日志
用户B的请求日志
用户A的后续日志
用户C的日志
```

非常乱，难以追踪一条完整链路。项目里一次请求的链路又很长：请求经过拦截器、Controller、Service、DAO 多层调用，如果每层各自打日志，又没有统一的链路标识和日志规范，排查问题时只能按时间盲搜，或者靠日志内容里碰巧出现的 userId、订单号去猜哪条日志属于同一次请求。

不解决会怎样？一个请求报错，想把它完整的日志从海量并发日志里捞出来几乎不可能；接口变慢，也无法快速判断是哪一层慢、慢了多少毫秒。日志埋点要么不统一、要么每处重复书写，最终日志沦为"有但查不了"的鸡肋。

## 二、用什么方法解决（方案对比）

围绕"给日志打标签"这件事，有几种做法：

- 手动拼接：每条日志手写 userId、IP、traceId。直观但容易漏，格式不统一，以后想改格式要改所有打印点。
- Filter/Interceptor 生成 requestId 放进 request attribute：能生成统一标识，但业务代码要层层传递，侵入性强。
- MDC（Mapped Diagnostic Context）：日志框架提供的线程级上下文存储机制，往里面放 key-value，日志模板自动输出，无需每处手动拼接。
- AOP 埋点 vs 每个方法手写耗时：AOP 一个切面覆盖所有接口，避免重复代码。

| 方案 | 做法 | 优点 | 缺点 |
| --- | --- | --- | --- |
| 手动拼接 | 每条日志手写上下文 | 简单直观 | 易漏、格式不一、改动成本高 |
| Filter + 参数传递 | 生成 requestId 层层传 | 不依赖日志框架 | 侵入业务代码、易漏传 |
| MDC | 线程级上下文 + 日志模板输出 | 无侵入、同线程自动带 | 新线程不继承、必须清理 |
| AOP 埋点 | 一个 @Around 切面统一计时 | 一处覆盖所有接口 | 需要理解切点与代理语义 |

本项目最终组合是：`TraceIdInterceptor`（order 0，最先执行）优先从请求头 `X-Trace-Id` 读取 traceId，没有就生成 UUID，写入 MDC 并回写到响应头；`RequestLogAspect` 用 @Around 拦截 Controller 层接口，统一记录请求方法、URI、处理方法、成功标志与耗时；日志模板（logback）中加上 `[traceId=%X{traceId}]`，让所有日志自动带上链路标识。

## 三、为什么需要这个技术（原理深入）

先讲 MDC 的本质。MDC 全称 Mapped Diagnostic Context（映射诊断上下文），是 SLF4J 提供的线程级上下文存储机制，底层就是 `ThreadLocal<Map<String, String>>`。它有三个关键特性：线程隔离，每个线程有自己的 MDC；自动传递，同一线程内的日志自动带上；不跨线程，新线程不会继承（除非手动传递）。用法很简单：

```java
MDC.put("traceId", "abc123");   // 设置上下文
log.info("用户发起请求");         // 日志自动带上 traceId
MDC.clear();                     // 清理，必须在 finally 中执行
```

因为基于 ThreadLocal，线程复用的坑也一样存在：如果不清理，线程池中的下一个任务会读到上一个任务的 traceId，造成脏数据污染，排查时反而被误导。所以清理环节（MDC.clear 或 MDC.remove）和写入同样重要。

traceId 的生成与透传由 TraceIdInterceptor 完成：优先读取请求头里的 X-Trace-Id（网关或上游透传的场景），没有就生成一个 UUID（去掉横线缩短长度）；写入 request attribute 供同请求内使用，回写响应头方便前后端联调，最后放进 MDC：

```java
@Component
public class TraceIdInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response, Object handler) {
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
    public void afterCompletion(HttpServletRequest request,
                                HttpServletResponse response,
                                Object handler, Exception ex) {
        MDC.remove(TraceIdConstants.TRACE_ID); // 请求结束清理
    }
}
```

接口耗时埋点用 AOP 的环绕通知实现：切点 `execution(public * com.hmdp.controller..*.*(..))` 覆盖 Controller 包下所有 public 方法。进入时记录开始时间，`joinPoint.proceed()` 执行原方法，正常返回用 log.info 记录耗时，抛出异常用 log.error 记录并把异常继续抛出，不影响原有的异常处理链路：

```java
@Around("execution(public * com.hmdp.controller..*.*(..))")
public Object logRequestCost(ProceedingJoinPoint joinPoint) throws Throwable {
    long startTime = System.currentTimeMillis();
    String traceId = MDC.get(TraceIdConstants.TRACE_ID);
    HttpServletRequest request =
            ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
    String requestMethod = request == null ? "N/A" : request.getMethod();
    String requestUri = request == null ? "N/A" : request.getRequestURI();
    String methodName = joinPoint.getSignature().toShortString();
    try {
        Object result = joinPoint.proceed();
        long cost = System.currentTimeMillis() - startTime;
        log.info("request finished traceId={}, method={}, uri={}, handler={}, cost={}ms",
                traceId, requestMethod, requestUri, methodName, cost);
        return result;
    } catch (Throwable ex) {
        long cost = System.currentTimeMillis() - startTime;
        log.error("request failed traceId={}, method={}, uri={}, handler={}, cost={}ms",
                traceId, requestMethod, requestUri, methodName, cost, ex);
        throw ex; // 异常继续抛出，不吞掉
    }
}
```

最后一步是日志模板。在 application.yaml 的 logging.pattern.console 里加上 MDC 变量：

```yaml
logging:
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} [traceId=%X{traceId}] - %msg%n"
```

输出效果类似：`2026-04-02 10:00:00 [http-nio-8080-exec-1] INFO UserService - 用户发起请求 [traceId=abc123]`，一条请求的所有日志都带着同一个 traceId。

整个请求生命周期是：进入时拦截器 preHandle 生成/透传 traceId 并写入 MDC；处理过程中 AOP 环绕通知记录耗时，任何一层打印的日志都自动带上 traceId；请求结束后拦截器 afterCompletion 移除 MDC，避免线程复用污染下一个请求。

## 四、不用这个技术怎么办（替代方案与当前边界）

不用 MDC：只能手动把 traceId 拼接进每条日志，容易漏、格式不统一，改格式要改所有打印点；或者把 traceId 作为参数层层传递，侵入业务代码。不用 AOP：每个 Controller 方法手写开始时间、结束时间、耗时日志，重复代码多、容易忘，统计口径也不统一。不用拦截器用 Filter：Filter 也能做同样的事，但拿不到 Spring MVC 的 handler 信息，而且拦截器可以精确控制顺序——本项目把 TraceIdInterceptor 放在 order 0，保证它在 JWT 拦截器之前执行，让认证日志也能带上 traceId。异步场景还要额外处理：新线程不会自动继承 MDC，可以用 Spring 的 TaskDecorator 在提交任务时复制上下文，或者在子线程里手动 put。

当前实现的边界要如实说明：这是"请求级"的链路日志，不是完整的分布式追踪——没有 Span 模型，没有跨服务采样，也没有把上下文传播到 Kafka、RabbitMQ 或线程池；traceId 的透传目前只在请求头（X-Trace-Id）和响应头之间闭环，跨服务、跨消息中间件的链路还没有打通；所有验证都在本地单机/容器环境完成，谈不上生产级的高并发验证。与 OpenTelemetry 这类标准分布式追踪相比，差距是明显的。

生产环境如何升级：引入 OpenTelemetry / Jaeger / SkyWalking 等链路追踪方案，用 W3C 的 traceparent 头在服务间传递 traceId + spanId；建立 Span 模型，记录每个子调用的耗时与依赖关系，统一采样策略控制成本；把上下文通过 MQ 消息头、HTTP 头或 TaskDecorator 传播到异步线程与消费者，真正做到跨进程的全链路追踪。

## 小结

1. 多线程并发下日志交叉混乱，需要统一的链路标识才能把一次请求的日志串联起来。
2. MDC 本质是 ThreadLocal<Map<String, String>>，日志模板自动输出、无侵入，但必须清理，否则线程池复用会污染数据。
3. TraceIdInterceptor 优先透传请求头里的 X-Trace-Id，没有则生成 UUID，并回写响应头方便联调。
4. AOP 环绕通知一处切面统一记录接口耗时，正常与异常分别落 info/error 日志，异常继续抛出。
5. 日志模板通过 %X{traceId} 输出 MDC 变量，全链路日志自动带标识。
6. 当前是请求级链路日志，没有 Span 和跨服务采样，升级方向是 OpenTelemetry / SkyWalking 等分布式追踪。
