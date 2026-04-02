package com.hmdp.Interceptor;

import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.JwtTool;
import com.hmdp.utils.UserHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Component
public class JwtTokenInterceptor implements HandlerInterceptor {

    private final JwtTool jwtTool;

    public JwtTokenInterceptor(JwtTool jwtTool) {
        this.jwtTool = jwtTool;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String token = resolveToken(request.getHeader("authorization"));
        if (StrUtil.isBlank(token)) {
            return true;
        }

        UserDTO userDTO = jwtTool.parseAccessToken(token);
        if (userDTO == null) {
            return true;
        }

        UserHolder.saveUser(userDTO);
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        UserHolder.removeUser();
    }

    private String resolveToken(String authorization) {
        if (StrUtil.isBlank(authorization)) {
            return null;
        }
        if (StrUtil.startWithIgnoreCase(authorization, "Bearer ")) {
            return authorization.substring(7).trim();
        }
        return authorization.trim();
    }
}