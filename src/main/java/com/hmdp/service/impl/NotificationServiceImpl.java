package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.NotificationDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.Notification;
import com.hmdp.entity.User;
import com.hmdp.mapper.NotificationMapper;
import com.hmdp.service.INotificationService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.NotificationConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class NotificationServiceImpl extends ServiceImpl<NotificationMapper, Notification> implements INotificationService {

    @Resource
    private IUserService userService;

    @Override
    public Result queryMyNotifications(Integer current) {
        Long userId = UserHolder.getUser().getId();
        Page<Notification> page = query()
                .eq("user_id", userId)
                .orderByDesc("create_time")
                .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
        List<Notification> records = page.getRecords();
        if (records.isEmpty()) {
            return Result.ok(Collections.emptyList(), 0L);
        }

        Set<Long> senderIds = records.stream()
                .map(Notification::getSenderUserId)
                .collect(Collectors.toSet());
        Map<Long, User> userMap = userService.listByIds(senderIds).stream()
                .collect(Collectors.toMap(User::getId, user -> user));

        List<NotificationDTO> data = records.stream().map(notification -> {
            NotificationDTO dto = BeanUtil.copyProperties(notification, NotificationDTO.class);
            dto.setFromUserId(notification.getSenderUserId());
            User sender = userMap.get(notification.getSenderUserId());
            if (sender != null) {
                dto.setFromUserName(sender.getNickName());
                dto.setFromUserIcon(sender.getIcon());
            }
            return dto;
        }).collect(Collectors.toList());
        return Result.ok(data, page.getTotal());
    }

    @Override
    public Result markAsRead(Long id) {
        Long userId = UserHolder.getUser().getId();
        boolean success = update()
                .set("read_status", NotificationConstants.READ_ALREADY)
                .eq("id", id)
                .eq("user_id", userId)
                .update();
        if (!success) {
            return Result.fail("Notification not found");
        }
        return Result.ok();
    }

    @Override
    public Result countUnread() {
        Long userId = UserHolder.getUser().getId();
        int count = query()
                .eq("user_id", userId)
                .eq("read_status", NotificationConstants.READ_UNREAD)
                .count();
        return Result.ok(count);
    }
}