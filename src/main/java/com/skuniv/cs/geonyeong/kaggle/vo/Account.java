package com.skuniv.cs.geonyeong.kaggle.vo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Account {
    private String id;
    private String displayName;
    private String aboutMe;
    private String age;
    private String createDate;
    private Integer upvotes;
    private Integer downVotes;
    private String profileImageUrl;
    private String websiteUrl;
}
