package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.LoginTokenDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.JwtTool;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_CODE_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_CODE_TTL;
import static com.hmdp.utils.RedisConstants.LOGIN_REFRESH_TOKEN_KEY;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private JwtTool jwtTool;

    @Value("${hmdp.jwt.refresh-token-ttl-days}")
    private long refreshTokenTtlDays;

    @Override
    public Result sendCode(String phone) {
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }
        String code = RandomUtil.randomNumbers(6);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        log.debug("发送验证码成功，验证码：{}", code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm) {
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }

        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (StrUtil.isBlank(cacheCode) || !StrUtil.equals(cacheCode, code)) {
            return Result.fail("验证码错误");
        }
        stringRedisTemplate.delete(LOGIN_CODE_KEY + phone);

        User user = query().eq("phone", phone).one();
        if (user == null) {
            user = createUserWithPhone(phone);
        }

        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        String accessToken = jwtTool.createAccessToken(userDTO);
        String refreshToken = UUID.randomUUID().toString(true);
        saveRefreshToken(refreshToken, userDTO);

        return Result.ok(buildTokenResult(userDTO, accessToken, refreshToken));
    }

    @Override
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
        stringRedisTemplate.expire(key, refreshTokenTtlDays, TimeUnit.DAYS);
        String accessToken = jwtTool.createAccessToken(userDTO);
        return Result.ok(buildTokenResult(userDTO, accessToken, refreshToken));
    }

    @Override
    public Result logout(String refreshToken) {
        if (StrUtil.isBlank(refreshToken)) {
            return Result.fail("refreshToken不能为空");
        }
        stringRedisTemplate.delete(LOGIN_REFRESH_TOKEN_KEY + refreshToken);
        return Result.ok();
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomNumbers(8));
        save(user);
        return user;
    }

    private void saveRefreshToken(String refreshToken, UserDTO userDTO) {
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) ->
                                fieldValue == null ? null : fieldValue.toString()));
        String key = LOGIN_REFRESH_TOKEN_KEY + refreshToken;
        stringRedisTemplate.opsForHash().putAll(key, userMap);
        stringRedisTemplate.expire(key, refreshTokenTtlDays, TimeUnit.DAYS);
    }

    private LoginTokenDTO buildTokenResult(UserDTO userDTO, String accessToken, String refreshToken) {
        return new LoginTokenDTO("Bearer", accessToken, jwtTool.getAccessTokenExpireSeconds(), refreshToken, userDTO);
    }
}