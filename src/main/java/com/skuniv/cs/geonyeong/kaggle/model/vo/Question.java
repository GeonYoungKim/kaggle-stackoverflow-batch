package com.skuniv.cs.geonyeong.kaggle.model.vo;

import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
@Builder
public class Question {
    private String questionId;
    private String title;
    private String body;
    private Integer answerCount;
    private Integer commentCount;
    private String createDate;
    private Integer favoriteCount;
    private String ownerDisplayName;
    private String ownerUserId;
    private Integer score;
    private String tags;
    private Integer viewCount;
    private String userAboutMe;
    private String userAge;
    private String userCreateDate;
    private Integer userUpVotes;
    private Integer userDownVotes;
    private String userProfileImageUrl;
    private String userWebsiteUrl;
    private List<Comment> commentList;
    private List<Link> linkList;
    private QnaJoin qnaJoin;
}
