package com.skuniv.cs.geonyeong.kaggle.vo;

import com.skuniv.cs.geonyeong.kaggle.vo.meta.QnAMeta;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper=false)
public class Question extends QnAMeta {
    private String id;
    private String title;
    private Integer answerCount;
    private Integer favoriteCount;
    private Integer viewCount;

    @Builder
    public Question(String body, String createDate, Integer score, Account account, Integer commentCount, String tags, List<Comment> commentList, List<Link> linkList, QnaJoin qnaJoin, String id, String title, Integer answerCount, Integer favoriteCount, Integer viewCount) {
        super(body, createDate, score, account, commentCount, tags, commentList, linkList, qnaJoin);
        this.id = id;
        this.title = title;
        this.answerCount = answerCount;
        this.favoriteCount = favoriteCount;
        this.viewCount = viewCount;
    }
}
