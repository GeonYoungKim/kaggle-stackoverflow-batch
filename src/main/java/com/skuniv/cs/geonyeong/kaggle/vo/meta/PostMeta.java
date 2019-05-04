package com.skuniv.cs.geonyeong.kaggle.vo.meta;

import com.skuniv.cs.geonyeong.kaggle.vo.Account;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
public abstract class PostMeta {
    private String body;
    private String createDate;
    private Integer score;
    private Account account;
}
