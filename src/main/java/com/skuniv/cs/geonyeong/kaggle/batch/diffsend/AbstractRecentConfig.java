package com.skuniv.cs.geonyeong.kaggle.batch.diffsend;

import com.skuniv.cs.geonyeong.kaggle.utils.TimeUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAccount;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroComment;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroLink;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractRecentConfig {

    private final String DETAIL_SPLIT_DELEMETER = "`";
    protected final String EMPTY_FIELD_VALUE = "None";

    protected AvroAccount createAvroAccount(String id, String name, String aboutMe, String age, String createDate, String upVotes, String downVotes, String profileImageUrl, String websiteUrl) throws ParseException {
        return AvroAccount.newBuilder()
                .setId(StringUtils.equals(EMPTY_FIELD_VALUE, id) ? "" : id)
                .setDisplayName(StringUtils.equals(EMPTY_FIELD_VALUE, name) ? "" : name)
                .setAboutMe(StringUtils.equals(EMPTY_FIELD_VALUE, aboutMe) ? "" : aboutMe)
                .setAge(StringUtils.equals(EMPTY_FIELD_VALUE, age) ? "" : age)
                .setCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, createDate) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(createDate))
                .setUpvotes(StringUtils.equals(EMPTY_FIELD_VALUE, upVotes) ? 0 : Integer.valueOf(upVotes))
                .setDownVotes(StringUtils.equals(EMPTY_FIELD_VALUE, downVotes) ? 0 : Integer.valueOf(downVotes))
                .setProfileImageUrl(StringUtils.equals(EMPTY_FIELD_VALUE, profileImageUrl) ? "" : profileImageUrl)
                .setWebsiteUrl(StringUtils.equals(EMPTY_FIELD_VALUE, websiteUrl) ? "" : websiteUrl)
                .build()
                ;
    }

    protected List<AvroComment> createAvroCommentList(String[] avroCommentSplit) {
        return Arrays.stream(avroCommentSplit)
                .filter(StringUtils::isNotBlank)
                .map(item -> {
                    String[] commentDetailSplit = item.split(DETAIL_SPLIT_DELEMETER, -1);
                    AvroComment avroComment = null;
                    try {
                        AvroAccount avroAccount = createAvroAccount(
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
                        avroComment = AvroComment.newBuilder()
                                .setCommentId(StringUtils.equals(EMPTY_FIELD_VALUE, commentDetailSplit[0]) ? "" : commentDetailSplit[0])
                                .setBody(StringUtils.equals(EMPTY_FIELD_VALUE, commentDetailSplit[1]) ? "" : commentDetailSplit[1])
                                .setCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, commentDetailSplit[2]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(commentDetailSplit[2]))
                                .setPostId(StringUtils.equals(EMPTY_FIELD_VALUE, commentDetailSplit[3]) ? "" : commentDetailSplit[3])
                                .setScore(StringUtils.equals(EMPTY_FIELD_VALUE, commentDetailSplit[6]) ? 0 : Integer.valueOf(commentDetailSplit[6]))
                                .setAccount(avroAccount)
                                .build();
                    } catch (ParseException e) {
                        log.error("date parse exception => {}", e);
                    }
                    return avroComment;
                })
                .collect(Collectors.toList());
    }

    protected List<AvroLink> createAvroLinkList(String[] avroLinkSplit) {
        return Arrays.stream(avroLinkSplit)
                .filter(StringUtils::isNotBlank)
                .map(item -> {
                    String[] linkDetailSplit = item.split(DETAIL_SPLIT_DELEMETER, -1);
                    return AvroLink.newBuilder()
                            .setLinkId(StringUtils.equals(EMPTY_FIELD_VALUE, linkDetailSplit[0]) ? "" : linkDetailSplit[0])
                            .setPostId(StringUtils.equals(EMPTY_FIELD_VALUE, linkDetailSplit[1]) ? "" : linkDetailSplit[1])
                            .setRelatedPostId(StringUtils.equals(EMPTY_FIELD_VALUE, linkDetailSplit[2]) ? "" : linkDetailSplit[2])
                            .build();
                })
                .collect(Collectors.toList());
    }
}
