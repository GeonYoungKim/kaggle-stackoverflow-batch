package com.skuniv.cs.geonyeong.kaggle.model.vo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Link {
    private Integer linkId;
    private Integer postId;
    private Integer relatedPostId;
}
