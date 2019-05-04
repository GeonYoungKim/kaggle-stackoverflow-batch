package com.skuniv.cs.geonyeong.kaggle.vo.meta;

import com.skuniv.cs.geonyeong.kaggle.vo.Account;
import com.skuniv.cs.geonyeong.kaggle.vo.Comment;
import com.skuniv.cs.geonyeong.kaggle.vo.Link;
import com.skuniv.cs.geonyeong.kaggle.vo.QnaJoin;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
public class QnAMeta extends PostMeta {
    private Integer commentCount;
    private String tags;
    private List<Comment> commentList;
    private List<Link> linkList;
    private QnaJoin qnaJoin;

    @Builder
    public QnAMeta(String body, String createDate, Integer score, Account account, Integer commentCount, String tags, List<Comment> commentList, List<Link> linkList, QnaJoin qnaJoin) {
        super(body, createDate, score, account);
        this.commentCount = commentCount;
        this.tags = tags;
        this.commentList = commentList;
        this.linkList = linkList;
        this.qnaJoin = qnaJoin;
    }
}
