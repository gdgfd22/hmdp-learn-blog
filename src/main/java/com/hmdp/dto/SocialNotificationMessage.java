package com.hmdp.dto;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public class SocialNotificationMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long receiverUserId;
    private Long senderUserId;
    private Long bizId;
    private String type;
    private String content;
    private LocalDateTime eventTime;
}
