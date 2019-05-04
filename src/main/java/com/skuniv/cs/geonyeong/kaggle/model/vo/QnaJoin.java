package com.skuniv.cs.geonyeong.kaggle.model.vo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class QnaJoin {
    private String name;
    private Integer parent;
}
