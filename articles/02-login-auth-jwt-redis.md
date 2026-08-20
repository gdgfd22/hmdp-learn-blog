# 登录认证方案演进：从 Session 到 Redis 共享会话，再到 JWT 双 Token + ThreadLocal

> 摘要：本文以黑马点评改造项目（hm-dianping）的登录认证为主线，梳理登录校验方案的三次演进：从 Servlet 的 Session，到 Redis 替换 Session 实现共享会话，再到 JWT accessToken + Redis refreshToken 双 Token + ThreadLocal 用户上下文。文章会讲清 JWT 结构与验签、两层拦截器的分工、ThreadLocal 的线程隔离原理，以及"退出只删 refreshToken、accessToken 仍有效到过期"这类边界。这是个人学习项目，所有验证都在本地单机/容器环境完成，取舍以学习原理为主。

## 一、为什么要这样做（业务背景与痛点）

HTTP 协议是"无状态"的：每一次请求都是独立的，下一次请求不会携带上一次请求的数据。用户通过浏览器完成登录后，再访问其他业务接口时，服务器并不知道他已经登录了。既然两次请求相互独立，就得自己想办法"记住"登录状态。

登录校验的思路分两步：第一步，登录成功后把"已登录"的标记保存起来；第二步，浏览器发起请求时在服务端统一拦截、校验标记。围绕这两步，衍生出会话技术（Cookie、Session、JWT）和统一拦截技术（Filter、Interceptor）两类选型。

课程初版用纯 Session 方案：验证码和登录用户都存进 Session，后续请求带上 JSessionId 取回用户。单机部署没问题，多实例部署就翻车：每个 Tomcat 各有一份自己的 Session，用户第一次请求落在第一台服务器、第二次却被负载均衡到第二台，第二台上没有他的 Session，登录拦截会误判为未登录。早期有"Session 拷贝"方案——任一服务器 Session 变了就同步给其他服务器——但每台都要保存完整的一份 Session 数据，压力过大，拷贝还有延迟。不解决共享问题，多实例下的登录态就完全不可信，用户只能反复重新登录。

## 二、用什么方法解决（方案对比）

先看会话技术的候选方案。Cookie 是 HTTP 协议原生支持的，浏览器自动解析 Set-Cookie、自动携带请求头，无需手动操作；但移动端 APP 无法使用，用户可以禁用，还不能跨域。Session 把数据放在服务端、相对安全，但集群下无法直接共享，且依赖 Cookie 携带会话标识，对 APP 不友好。JWT 本质就是一个字符串，支持 PC 端和移动端，天然解决集群认证问题，也无需在服务器端存储；代价是生成、传递、校验都要自己实现。

| 方案 | 存储位置 | 优点 | 缺点 |
| --- | --- | --- | --- |
| Cookie | 浏览器 | HTTP 原生支持、浏览器自动携带 | APP 无法使用、可被禁用、不能跨域、不安全 |
| Session | 服务器 | 数据在服务端、较安全 | 集群下无法共享、依赖 Cookie、APP 不友好 |
| JWT | 客户端无状态 | 支持 PC/移动端、解决集群认证、服务端无存储压力 | 生成/传递/校验都要自己实现 |

第一个中间方案是用 Redis 替代 Session。Session 的替代方案要满足三个条件：数据共享、内存存储、key/value 结构，Redis 恰好全部满足。登录成功后生成随机 UUID 作为 token，把用户信息以 Hash 结构存进 Redis 并设置过期时间；校验时携带 token 取出 value，存在则放行、不存在则拦截。这个方案解决了共享问题，但每次请求都要查一次 Redis，属于有状态认证。

最终方案采用 JWT accessToken + Redis refreshToken 双 Token：accessToken 负责接口认证，refreshToken 存 Redis 负责会话管理。普通接口鉴权只验 JWT、不查 Redis，保持无状态和低延迟；续期、退出登录、会话过期这些会话级控制都落在 refreshToken 上，可控可删。核心理由是把无状态快速鉴权和有状态会话管理分开，各取所长。

## 三、为什么需要这个技术（原理深入）

先理解 JWT 的结构：header.payload.signature 三段，用点分隔。header 声明签名算法和类型，payload 存放业务载荷，signature 是用密钥对前两段签名得到的结果。项目用 HS256（HMAC-SHA256）签名，签发方与验签方共享密钥。注意 JWT 只是 Base64URL 编码而非加密，客户端能解码看到载荷，所以载荷里只放用户 ID、昵称、头像和时间字段，不存敏感信息。

双 Token 完整链路：登录成功签发 accessToken，同时生成 refreshToken 写入 Redis，前端保存两个 token。之后每次请求在请求头带 `Authorization: Bearer accessToken`，拦截器解析 JWT，成功就把用户写入 ThreadLocal，业务代码通过 UserHolder 获取当前用户。accessToken 过期返回 401 时，前端自动带 refreshToken 调 `/user/refresh`，后端校验后重新签发 accessToken，实现自动续期闭环。

登录时签发双 Token 的核心代码（UserServiceImpl）：

```java
public Result login(LoginFormDTO loginForm) {
    String phone = loginForm.getPhone();
    if (RegexUtils.isPhoneInvalid(phone)) {
        return Result.fail("手机号格式错误");
    }
    // 从 Redis 取验证码并校验
    String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
    if (StrUtil.isBlank(cacheCode) || !StrUtil.equals(cacheCode, loginForm.getCode())) {
        return Result.fail("验证码错误");
    }
    User user = query().eq("phone", phone).one();
    if (user == null) {
        user = createUserWithPhone(phone); // 首次登录自动注册
    }
    UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
    // 双 Token：accessToken 无状态认证，refreshToken 存 Redis 管会话
    String accessToken = jwtTool.createAccessToken(userDTO);
    String refreshToken = UUID.randomUUID().toString(true);
    saveRefreshToken(refreshToken, userDTO);
    return Result.ok(buildTokenResult(userDTO, accessToken, refreshToken));
}
```

refreshToken 续签时，从 Redis 的 Hash 里取出用户信息，重新签发 accessToken，并顺带延长会话有效期：

```java
public Result refreshAccessToken(String refreshToken) {
    if (StrUtil.isBlank(refreshToken)) {
        return Result.fail("refreshToken不能为空");
    }
    String key = LOGIN_REFRESH_TOKEN_KEY + refreshToken;
    Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);
    if (userMap.isEmpty()) {
        return Result.fail("登录已过期，请重新登录");
    }
    UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
    stringRedisTemplate.expire(key, refreshTokenTtlDays, TimeUnit.DAYS); // 续期
    String accessToken = jwtTool.createAccessToken(userDTO);
    return Result.ok(buildTokenResult(userDTO, accessToken, refreshToken));
}
```

拦截器流程分两层。第一层 `JwtTokenInterceptor` 定位"认人"：取请求头 authorization、去掉 `Bearer ` 前缀，调 JwtTool 验签解析；没 token 直接放行，token 无效也先放行，只有合法才把 UserDTO 塞进 UserHolder——目标不是拦人，而是"尽量解析用户"。第二层 `LoginInterceptor` 只做一件事：看 UserHolder 有没有用户，没有就返回 401，有就放行。MvcConfig 先注册 JWT 拦截器再注册登录拦截器。

```java
@Component
public class JwtTokenInterceptor implements HandlerInterceptor {

    private final JwtTool jwtTool;

    public JwtTokenInterceptor(JwtTool jwtTool) {
        this.jwtTool = jwtTool;
    }

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response, Object handler) {
        String token = resolveToken(request.getHeader("authorization"));
        if (StrUtil.isBlank(token)) {
            return true;            // 没有 token，放行
        }
        UserDTO userDTO = jwtTool.parseAccessToken(token);
        if (userDTO == null) {
            return true;            // token 无效，也先放行
        }
        UserHolder.saveUser(userDTO); // 解析成功，把用户塞进 ThreadLocal
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request,
                                HttpServletResponse response,
                                Object handler, Exception ex) {
        UserHolder.removeUser();    // 请求结束清理，防止线程复用串号
    }
    // resolveToken(...)：去掉 "Bearer " 前缀后返回 token
}
```

为什么要用 ThreadLocal 而不是全局变量？全局变量被所有请求共享，线程不安全——A 用户刚存进去，B 用户的请求可能马上覆盖掉，用户数据就串了。而 ThreadLocal 是线程隔离的：线程 A 只能看到自己的 user，线程 B 只能看到自己的 user。一个经典易混淆点：ThreadLocal 并不是一个 Thread，恰恰相反，是 Thread 持有自己的 ThreadLocalMap 局部变量，ThreadLocal 是操作这个局部变量的"全局工具类"。在 Spring MVC 里，一次 HTTP 请求通常由一个线程处理，所以能把"当前请求对应的登录用户"安全绑定到"当前处理请求的线程"上，让 Controller、Service、Mapper 任意位置都能拿到用户，而不用层层传 userId。

UserHolder 就是对 ThreadLocal 的一层封装：

```java
public class UserHolder {
    private static final ThreadLocal<UserDTO> tl = new ThreadLocal<>();

    public static void saveUser(UserDTO user) {
        tl.set(user);
    }

    public static UserDTO getUser() {
        return tl.get();
    }

    public static void removeUser() {
        tl.remove();
    }
}
```

但"线程隔离"也带来一条必须遵守的纪律：一定要 removeUser。Web 容器线程是复用的，请求结束线程并不销毁；不清理的话，下一个请求复用这个线程时可能拿到上一个用户的数据，造成严重串号。所以标准流程是：拦截器里 saveUser、业务里 getUser、请求结束 removeUser。

## 四、不用这个技术怎么办（替代方案与当前边界）

如果不做双 Token，可退回 Session + Redis：随机 token 当 key、用户信息存 Redis，每次请求都查 Redis 判断登录态，代价是多一次网络往返、属于有状态认证。如果不做 ThreadLocal，就得把 userId 从 controller 一路传到 service、dao，每层加参数，繁琐易漏；用全局变量则直接引入线程安全问题。如果只用单个 token，就没有续签机制，过期只能重新登录，服务端也无法控制会话退出与失效。

当前实现边界需要如实说明：退出只删 Redis 里的 refreshToken，已签发的 accessToken 在普通鉴权中不查 Redis，仍有效到 JWT 自身过期；refreshToken 不轮换，并发刷新可同时签发多个 accessToken，属学习版实现；JWT 载荷只有用户 ID、昵称、头像和时间字段，不存敏感信息；签名用 HS256 对称密钥，demo secret 写在 YAML 里，只适合本地学习；Redis 宕机时，accessToken 有效且只依赖数据库的请求仍能鉴权，但登录、验证码、刷新以及缓存、秒杀等功能都会失败，没有熔断与降级保护；异步线程和 MQ 消费者拿不到 ThreadLocal，业务身份必须通过任务参数或消息体显式传递；项目也未引入 Spring Security，自定义拦截器是为了聚焦认证原理。

生产升级方向：refreshToken 做轮换，用 Redis Lua 脚本原子完成"删旧 token + 重建新 token"，保证一次性使用，记录旧 token 重用并撤销整个会话族；需立即封禁时，缩短 accessToken TTL，增加 token 版本号、用户封禁状态或黑名单校验；密钥从环境变量或密钥管理系统注入、限制读取权限，用 kid 或密钥版本支持灰度轮换；若引入 Spring Security，把 JWT 解析放进 Security Filter Chain，用户封装成 Authentication 写入 SecurityContext，用授权规则替代登录拦截器。

## 小结

1. HTTP 无状态决定了登录校验必须"保存登录标记 + 统一拦截"两步走。
2. Session 集群下无法共享，"Session 拷贝"压力大且有延迟；Redis 以数据共享、内存存储、key/value 三个特性成为替代方案。
3. 最终方案是 JWT accessToken 负责接口认证（无状态、不查 Redis），Redis refreshToken 负责会话管理（可续期、可退出）。
4. 拦截器分两层：JwtTokenInterceptor 负责"认人"，LoginInterceptor 负责"拦未登录的人"。
5. ThreadLocal 线程隔离，UserHolder 让业务代码免去层层传 userId，但必须 removeUser 防止线程复用串号。
6. 当前边界：退出只删 refreshToken、refreshToken 不轮换、密钥为本地 demo，均为学习版定位。
