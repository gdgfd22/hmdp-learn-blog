package com.hmdp.controller;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.LoginTokenDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/user")
public class UserController {

    @Resource
    private IUserService userService;

    @Resource
    private IUserInfoService userInfoService;

    @PostMapping("code")
    public Result sendCode(@RequestParam("phone") String phone) {
        return userService.sendCode(phone);
    }

    @PostMapping("/login")
    public Result login(@RequestBody LoginFormDTO loginForm, HttpServletResponse response) {
        return adaptTokenResult(userService.login(loginForm), response);
    }

    @PostMapping("/refresh")
    public Result refresh(@RequestHeader(value = "refresh-token", required = false) String refreshToken,
                          HttpServletResponse response) {
        return adaptTokenResult(userService.refreshAccessToken(refreshToken), response);
    }

    @PostMapping("/logout")
    public Result logout(@RequestHeader(value = "refresh-token", required = false) String refreshToken) {
        return userService.logout(refreshToken);
    }

    @GetMapping("/me")
    public Result me() {
        return Result.ok(UserHolder.getUser());
    }

    @GetMapping("/info/{id}")
    public Result info(@PathVariable("id") Long userId) {
        UserInfo info = userInfoService.getById(userId);
        if (info == null) {
            return Result.ok();
        }
        info.setCreateTime(null);
        info.setUpdateTime(null);
        return Result.ok(info);
    }

    @GetMapping("/{id}")
    public Result queryUserById(@PathVariable("id") Long userId) {
        User user = userService.getById(userId);
        if (user == null) {
            return Result.ok();
        }
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        return Result.ok(userDTO);
    }

    private Result adaptTokenResult(Result result, HttpServletResponse response) {
        if (!Boolean.TRUE.equals(result.getSuccess()) || !(result.getData() instanceof LoginTokenDTO)) {
            return result;
        }
        LoginTokenDTO tokenDTO = (LoginTokenDTO) result.getData();
        response.setHeader("refresh-token", tokenDTO.getRefreshToken());
        response.setHeader("token-type", tokenDTO.getTokenType());
        response.setHeader("access-token-expires-in", String.valueOf(tokenDTO.getExpiresIn()));
        return Result.ok(tokenDTO.getAccessToken());
    }
}
