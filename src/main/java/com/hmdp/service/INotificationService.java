package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.Result;
import com.hmdp.entity.Notification;

public interface INotificationService extends IService<Notification> {

    Result queryMyNotifications(Integer current);

    Result markAsRead(Long id);

    Result countUnread();
}
