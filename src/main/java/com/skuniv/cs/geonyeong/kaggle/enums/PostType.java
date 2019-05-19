package com.skuniv.cs.geonyeong.kaggle.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum PostType {
    ANSWER("answer"),
    QUESTION("question")
    ;

    private String type;
}
