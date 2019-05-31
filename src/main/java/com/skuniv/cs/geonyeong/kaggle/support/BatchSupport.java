package com.skuniv.cs.geonyeong.kaggle.support;

import com.skuniv.cs.geonyeong.kaggle.utils.TimeUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.Account;
import com.skuniv.cs.geonyeong.kaggle.vo.Comment;
import com.skuniv.cs.geonyeong.kaggle.vo.Link;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class BatchSupport {
    public static final String EMPTY_FIELD_VALUE = "None";

    public static List<Comment> createCommentList(String[] commentSplit) {
        return Arrays.stream(commentSplit)
            .filter(StringUtils::isNotBlank)
            .map(item -> {
                String[] commentDetailSplit = item.split("`", -1);
                Comment comment = null;
                try {
                    Account account = createAccount(
                        commentDetailSplit[4], // id
                        commentDetailSplit[5], // name
                        commentDetailSplit[7], // aboutMe
                        commentDetailSplit[8], // age
                        commentDetailSplit[9], // createDate
                        commentDetailSplit[10], // upvotes
                        commentDetailSplit[11], // downvotes
                        commentDetailSplit[12], // profileImageUrl
                        commentDetailSplit[13] // websiteUrl
                    );
                    comment = Comment.builder()
                        .commentId(commentDetailSplit[0])
                        .body(commentDetailSplit[1])
                        .createDate(TimeUtil.toStr(commentDetailSplit[2]))
                        .postId(commentDetailSplit[3])
                        .score(Integer.valueOf(commentDetailSplit[6]))
                        .account(account)
                        .build();
                } catch (ParseException e) {
                    log.error("date parse exception => {}", e);
                }
                return comment;
            })
            .collect(Collectors.toList());
    }

    public static Account createAccount(String id, String name, String aboutMe, String age,
        String createDate, String upVotes, String downVotes, String profileImageUrl,
        String websiteUrl) throws ParseException {
        return Account.builder()
            .id(StringUtils.equals(EMPTY_FIELD_VALUE, id) ? "" : id)
            .displayName(StringUtils.equals(EMPTY_FIELD_VALUE, name) ? "" : name)
            .aboutMe(StringUtils.equals(EMPTY_FIELD_VALUE, aboutMe) ? "" : aboutMe)
            .age(StringUtils.equals(EMPTY_FIELD_VALUE, age) ? "" : age)
            .createDate(
                StringUtils.equals(EMPTY_FIELD_VALUE, createDate) ? TimeUtil.toStr(new Date())
                    : TimeUtil.toStr(createDate))
            .upvotes(StringUtils.equals(EMPTY_FIELD_VALUE, upVotes) ? 0 : Integer.valueOf(upVotes))
            .downVotes(
                StringUtils.equals(EMPTY_FIELD_VALUE, downVotes) ? 0 : Integer.valueOf(downVotes))
            .profileImageUrl(
                StringUtils.equals(EMPTY_FIELD_VALUE, profileImageUrl) ? "" : profileImageUrl)
            .websiteUrl(StringUtils.equals(EMPTY_FIELD_VALUE, websiteUrl) ? "" : websiteUrl)
            .build()
            ;
    }

    public static List<Link> createLinkList(String[] linkSplit) {
        return Arrays.stream(linkSplit)
            .filter(StringUtils::isNotBlank)
            .map(item -> {
                String[] linkDetailSplit = item.split("`", -1);
                return Link.builder()
                    .linkId(linkDetailSplit[0])
                    .postId(linkDetailSplit[1])
                    .relatedPostId(linkDetailSplit[2])
                    .build();
            })
            .collect(Collectors.toList());
    }

}
