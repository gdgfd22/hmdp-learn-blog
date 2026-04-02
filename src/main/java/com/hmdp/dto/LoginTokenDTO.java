package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginTokenDTO {
    private String tokenType;
    private String accessToken;
    private Long expiresIn;
    private String refreshToken;
    private UserDTO user;
}