package com.skuniv.cs.geonyeong.kaggle.vo;

import com.skuniv.cs.geonyeong.kaggle.vo.meta.PostMeta;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class Comment extends PostMeta {
    private String commentId;
    private String postId;

    @Builder
    public Comment(String body, String createDate, Integer score, Account account, String commentId, String postId) {
        super(body, createDate, score, account);
        this.commentId = commentId;
        this.postId = postId;
    }
}
