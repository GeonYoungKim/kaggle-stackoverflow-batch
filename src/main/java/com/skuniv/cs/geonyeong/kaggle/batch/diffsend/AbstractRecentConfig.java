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
public  abstract class AbstractRecentConfig {
    protected final String EMPTY_FIELD_VALUE = "None";

    protected AvroAccount createAvroAccount(String id, String name, String aboutMe, String age, String createDate, String upVotes, String downVotes, String profileImageUrl, String websiteUrl) throws ParseException {
        return AvroAccount.newBuilder()
                .setId(id)
                .setDisplayName(name)
                .setAboutMe(aboutMe)
                .setAge(age)
                .setCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, createDate) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(createDate))
                .setUpvotes(Integer.valueOf(upVotes))
                .setDownVotes(Integer.valueOf(downVotes))
                .setProfileImageUrl(profileImageUrl)
                .setWebsiteUrl(websiteUrl)
                .build()
                ;
    }

    protected List<AvroComment> createAvroCommentList(String[] avroCommentSplit) {
        return Arrays.stream(avroCommentSplit)
                .filter(StringUtils::isNotBlank)
                .map(item -> {
                    String[] commentDetailSplit = item.split("`", -1);
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
                                .setCommentId(commentDetailSplit[0])
                                .setBody(commentDetailSplit[1])
                                .setCreateDate(TimeUtil.toStr(commentDetailSplit[2]))
                                .setPostId(commentDetailSplit[3])
                                .setScore(Integer.valueOf(commentDetailSplit[7]))
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
                    String[] linkDetailSplit = item.split("`", -1);
                    return AvroLink.newBuilder()
                            .setLinkId(linkDetailSplit[0])
                            .setPostId(linkDetailSplit[1])
                            .setRelatedPostId(linkDetailSplit[2])
                            .build();
                })
                .collect(Collectors.toList());
    }
}
