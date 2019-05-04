package com.skuniv.cs.geonyeong.kaggle.model.vo;

import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@Builder
public class Comment {
    private String commentId;
    private String text;
    private String createDate;
    private String postId;
    private String userId;
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
