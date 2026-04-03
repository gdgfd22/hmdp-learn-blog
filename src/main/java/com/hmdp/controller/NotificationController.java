package com.hmdp.controller;

import com.hmdp.dto.Result;
import com.hmdp.service.INotificationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/notification")
public class NotificationController {

    @Resource
    private INotificationService notificationService;

    @GetMapping
    public Result queryMyNotifications(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        return notificationService.queryMyNotifications(current);
    }

    @GetMapping("/unread/count")
    public Result countUnread() {
        return notificationService.countUnread();
    }

    @PutMapping("/read/{id}")
    public Result markAsRead(@PathVariable("id") Long id) {
        return notificationService.markAsRead(id);
    }
}
