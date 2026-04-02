package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class JwtTool {

    private static final Base64.Encoder URL_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder URL_DECODER = Base64.getUrlDecoder();
    private static final String JWT_HEADER = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";

    @Value("${hmdp.jwt.secret}")
    private String secret;

    @Value("${hmdp.jwt.access-token-ttl-minutes}")
    private long accessTokenTtlMinutes;

    public String createAccessToken(UserDTO user) {
        long nowSeconds = System.currentTimeMillis() / 1000;
        long expSeconds = nowSeconds + accessTokenTtlMinutes * 60;

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("sub", String.valueOf(user.getId()));
        payload.put("id", user.getId());
        payload.put("nickName", user.getNickName());
        payload.put("icon", user.getIcon());
        payload.put("iat", nowSeconds);
        payload.put("exp", expSeconds);

        String headerPart = base64UrlEncode(JWT_HEADER);
        String payloadPart = base64UrlEncode(JSONUtil.toJsonStr(payload));
        String content = headerPart + "." + payloadPart;
        return content + "." + sign(content);
    }

    public UserDTO parseAccessToken(String token) {
        if (StrUtil.isBlank(token)) {
            return null;
        }

        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            return null;
        }

        String content = parts[0] + "." + parts[1];
        String expectedSignature = sign(content);
        if (!MessageDigest.isEqual(
                expectedSignature.getBytes(StandardCharsets.UTF_8),
                parts[2].getBytes(StandardCharsets.UTF_8))) {
            return null;
        }

        String payloadJson = new String(URL_DECODER.decode(parts[1]), StandardCharsets.UTF_8);
        JSONObject payload = JSONUtil.parseObj(payloadJson);
        Long exp = payload.getLong("exp");
        long nowSeconds = System.currentTimeMillis() / 1000;
        if (exp == null || exp <= nowSeconds) {
            return null;
        }

        UserDTO userDTO = new UserDTO();
        userDTO.setId(payload.getLong("id"));
        userDTO.setNickName(payload.getStr("nickName"));
        userDTO.setIcon(payload.getStr("icon"));
        return userDTO;
    }

    public long getAccessTokenExpireSeconds() {
        return accessTokenTtlMinutes * 60;
    }

    private String sign(String content) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(keySpec);
            byte[] digest = mac.doFinal(content.getBytes(StandardCharsets.UTF_8));
            return URL_ENCODER.encodeToString(digest);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to sign jwt token", e);
        }
    }

    private String base64UrlEncode(String value) {
        return URL_ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}