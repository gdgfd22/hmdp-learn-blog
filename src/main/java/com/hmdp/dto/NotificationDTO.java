package com.hmdp.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class NotificationDTO {

    private Long id;
    private String type;
    private String content;
    private Long bizId;
    private Integer readStatus;
    private Long fromUserId;
    private String fromUserName;
    private String fromUserIcon;
    private LocalDateTime createTime;
}
