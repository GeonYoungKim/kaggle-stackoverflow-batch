package com.skuniv.cs.geonyeong.kaggle.model.vo;

import com.skuniv.cs.geonyeong.kaggle.model.vo.Comment;
import com.skuniv.cs.geonyeong.kaggle.model.vo.Link;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
@Builder
public class Answer {
    private Integer answerId;
    private String body;
    private Integer commentCount;
    private String createDate;
    private String ownerDisplayName;
    private Integer ownerUserId;
    private Integer parentId;
    private Integer score;
    private String tags;
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
