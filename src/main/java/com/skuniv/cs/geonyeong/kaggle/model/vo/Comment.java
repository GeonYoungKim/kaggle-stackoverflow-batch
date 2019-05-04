package com.skuniv.cs.geonyeong.kaggle.model.vo;

import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
public class Comment {
    private Integer commentId;
    private String text;
    private String createDate;
    private Integer postId;
    private Integer userId;
    private String userDisplayName;
    private Integer score;
    private String userAboutMe;
    private String userAge;
    private String userCreateDate;
    private Integer userUpvotes;
    private Integer userDownVotes;
    private String userProfileImageUrl;
    private String userWebsiteUrl;
}
