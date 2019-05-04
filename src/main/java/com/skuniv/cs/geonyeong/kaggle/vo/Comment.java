package com.skuniv.cs.geonyeong.kaggle.vo;

import com.skuniv.cs.geonyeong.kaggle.vo.meta.PostMeta;
import lombok.Builder;
import lombok.Data;

@Data
public class Comment extends PostMeta {
    private String commentId;
    private String postId;
    private Account account;

    @Builder
    public Comment(String body, String createDate, Integer score, Account account, String commentId, String postId, Account account1) {
        super(body, createDate, score, account);
        this.commentId = commentId;
        this.postId = postId;
        this.account = account1;
    }
}
