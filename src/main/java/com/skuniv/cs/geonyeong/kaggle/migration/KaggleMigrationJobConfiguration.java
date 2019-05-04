package com.skuniv.cs.geonyeong.kaggle.migration;

import com.google.gson.Gson;
import com.skuniv.cs.geonyeong.kaggle.configuration.EsConfiguration;
import com.skuniv.cs.geonyeong.kaggle.utils.TimeUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
@Import({EsConfiguration.class})
public class KaggleMigrationJobConfiguration extends DefaultBatchConfigurer {
    private final static Gson gson = new Gson();

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index}")
    private String esIndexName;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.type}")
    private String esType;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final RestHighLevelClient restHighLevelClient;

    private final Integer CHUNCK_SIZE = 200;
    private final String ANSWER_JOIN_NAME = "answer";
    private final String QUESTION_JOIN_NAME = "question";
    private final String EMPTY_FIELD_VALUE = "None";
    private final String HIVE_DELEMETER_FIRST = "\001";
    private final String HIVE_DELEMETER_SECOND = "\002";

    @Bean
    public Job kaggleMigrationJob() {
        return jobBuilderFactory.get("kaggleMigrationJob")
                .start(kaggleMigrationQuestionStep())
                .next(kaggleMigrationAnswerStep())
                .build()
                ;
    }

    @Bean
    public Step kaggleMigrationAnswerStep() {
        return stepBuilderFactory.get("kaggleMigrationAnswerStep")
                .<Answer, Answer>chunk(CHUNCK_SIZE)
                .reader(multiResourceAnswerItemReader(null))
                .writer(itemAnswerWriter(esIndexName))
                .build()
                ;
    }

    @Bean
    public Step kaggleMigrationQuestionStep() {
        return stepBuilderFactory.get("kaggleMigrationQuestionStep")
                .<Question, Question>chunk(CHUNCK_SIZE)
                .reader(multiResourceQuestionItemReader(null))
                .writer(itemQuestionWriter(esIndexName))
                .build()
                ;
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<Answer> multiResourceAnswerItemReader(@Value("#{jobParameters[answerPath]}") String answerPath) {
        MultiResourceItemReader<Answer> multiResourceItemReader = createMultiResourceItemReader(answerPath);
        multiResourceItemReader.setDelegate(answerReader());
        return multiResourceItemReader;
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<Question> multiResourceQuestionItemReader(@Value("#{jobParameters[questionPath]}") String questionPath) {
        MultiResourceItemReader<Question> multiResourceItemReader = createMultiResourceItemReader(questionPath);
        multiResourceItemReader.setDelegate(questionReader());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<Question> questionReader() {
        FlatFileItemReader<Question> reader = new FlatFileItemReader<Question>();
        reader.setLineMapper(new LineMapper<Question>() {
            @Override
            public Question mapLine(String line, int lineNumber) throws Exception {
                String[] questionSplit = line.split(HIVE_DELEMETER_FIRST, -1);
                String[] commentSplit = questionSplit[19].split(HIVE_DELEMETER_SECOND, -1);
                String[] linkSplit = questionSplit[20].split(HIVE_DELEMETER_SECOND, -1);
                List<Comment> commentList = createCommentList(commentSplit);
                List<Link> linkList = createLinkList(linkSplit);
                Account account = createAccount(
                        questionSplit[8], // id
                        questionSplit[7], // name
                        questionSplit[12], // aboutme
                        questionSplit[13], // age
                        questionSplit[14], // createDate
                        questionSplit[15], // upvotes
                        questionSplit[16], // downvotes
                        questionSplit[17], // profileImageUrl
                        questionSplit[18] // websiteUrl
                );
                Question question = Question.builder()
                        .id(questionSplit[0])
                        .title(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[1]) ? "" : questionSplit[1])
                        .body(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[2]) ? "" : questionSplit[2])
                        .answerCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[3]) ? 0 : Integer.valueOf(questionSplit[3]))
                        .commentCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[4]) ? 0 : Integer.valueOf(questionSplit[4]))
                        .createDate(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[5]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(questionSplit[5]))
                        .favoriteCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[6]) ? 0 : Integer.valueOf(questionSplit[6]))
                        .score(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[9]) ? 0 : Integer.valueOf(questionSplit[9]))
                        .tags(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[10]) ? "" : questionSplit[10])
                        .viewCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[11]) ? 0 : Integer.valueOf(questionSplit[11]))
                        .account(account)
                        .commentList(commentList)
                        .linkList(linkList)
                        .qnaJoin(QnaJoin.builder().name(QUESTION_JOIN_NAME).build())
                        .build();
                return question;
            }
        });
        return reader;
    }

    @Bean
    public FlatFileItemReader<Answer> answerReader() {
        FlatFileItemReader<Answer> reader = new FlatFileItemReader<Answer>();
        reader.setLineMapper(new LineMapper<Answer>() {
            @Override
            public Answer mapLine(String line, int lineNumber) throws Exception {
                String[] answerSplit = line.split(HIVE_DELEMETER_FIRST, -1);
                String[] commentSplit = answerSplit[16].split(HIVE_DELEMETER_SECOND, -1);
                String[] linkSplit = answerSplit[17].split(HIVE_DELEMETER_SECOND, -1);
                List<Comment> commentList = createCommentList(commentSplit);
                List<Link> linkList = createLinkList(linkSplit);
                Account account = createAccount(
                        answerSplit[5], // id
                        answerSplit[4], // anme
                        answerSplit[9], // aboutMe
                        answerSplit[10], // age
                        answerSplit[11], // createDate
                        answerSplit[12], // upvotes
                        answerSplit[13], // downvotes
                        answerSplit[14], // profileImageUrl
                        answerSplit[15] // websiteUrl
                );
                Answer answer = Answer.builder()
                        .id(answerSplit[0])
                        .body(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[1]) ? "" : answerSplit[1])
                        .commentCount(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[2]) ? 0 : Integer.valueOf(answerSplit[2]))
                        .createDate(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[3]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(answerSplit[3]))
                        .parentId(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[6]) ? "" : answerSplit[6])
                        .score(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[7]) ? 0 : Integer.valueOf(answerSplit[7]))
                        .tags(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[8]) ? "" : answerSplit[8])
                        .account(account)
                        .commentList(commentList)
                        .linkList(linkList)
                        .qnaJoin(
                                QnaJoin.builder()
                                        .name(ANSWER_JOIN_NAME)
                                        .parent(answerSplit[6])
                                        .build()
                        )
                        .build();
                return answer;
            }
        });
        return reader;
    }

    @Bean
    public ItemWriter<Answer> itemAnswerWriter(String indexName) {
        ItemWriter<Answer> answerItemWriter = new ItemWriter<Answer>() {
            @Override
            public void write(List<? extends Answer> items) throws Exception {
                BulkRequest bulkRequest = new BulkRequest();
                items.forEach(item -> {
                            bulkRequest.add(
                                    new IndexRequest(indexName)
                                            .source(gson.toJson(item), XContentType.JSON)
                                            .type(esType)
                                            .id(String.valueOf(item.getId()))
                                            .routing(String.valueOf(item.getParentId()))
                            );
                        }
                );
                BulkResponse bulkItemResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (bulkItemResponses.hasFailures()) {
                    log.info("answer response => {}", bulkItemResponses.buildFailureMessage());
                }
            }
        };
        return answerItemWriter;
    }

    @Bean
    public ItemWriter<Question> itemQuestionWriter(String indexName) {
        ItemWriter<Question> answerItemWriter = new ItemWriter<Question>() {
            @Override
            public void write(List<? extends Question> items) throws Exception {
                BulkRequest bulkRequest = new BulkRequest();

                items.forEach(item -> {
                            bulkRequest.add(
                                    new IndexRequest(indexName)
                                            .source(gson.toJson(item), XContentType.JSON)
                                            .type(esType)
                                            .id(String.valueOf(item.getId()))
                                            .routing(String.valueOf(item.getId()))
                            );
                        }
                );
                BulkResponse bulkItemResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (bulkItemResponses.hasFailures()) {
                    log.info("question response => {}", bulkItemResponses.buildFailureMessage());
                }
            }
        };
        return answerItemWriter;
    }

    private List<Link> createLinkList(String[] linkSplit) {
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


    private List<Comment> createCommentList(String[] commentSplit) {
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

    private MultiResourceItemReader createMultiResourceItemReader(String filePath) {
        ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = null;
        try {
            resources = patternResolver.getResources("file:" + filePath);
        } catch (IOException e) {
            log.error("resources get error");
        }
        MultiResourceItemReader<String> multiResourceItemReader = new MultiResourceItemReader<String>();
        multiResourceItemReader.setResources(resources);
        return multiResourceItemReader;
    }

    private Account createAccount(String id, String name, String aboutMe, String age, String createDate, String upVotes, String downVotes, String profileImageUrl, String websiteUrl) throws ParseException {
        return Account.builder()
                .id(StringUtils.equals(EMPTY_FIELD_VALUE, id) ? "" : id)
                .displayName(StringUtils.equals(EMPTY_FIELD_VALUE, name) ? "" : name)
                .aboutMe(StringUtils.equals(EMPTY_FIELD_VALUE, aboutMe) ? "" : aboutMe)
                .age(StringUtils.equals(EMPTY_FIELD_VALUE, age) ? "" : age)
                .createDate(StringUtils.equals(EMPTY_FIELD_VALUE, createDate) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(createDate))
                .upvotes(StringUtils.equals(EMPTY_FIELD_VALUE, upVotes) ? 0 : Integer.valueOf(upVotes))
                .downVotes(StringUtils.equals(EMPTY_FIELD_VALUE, downVotes) ? 0 : Integer.valueOf(downVotes))
                .profileImageUrl(StringUtils.equals(EMPTY_FIELD_VALUE, profileImageUrl) ? "" : profileImageUrl)
                .websiteUrl(StringUtils.equals(EMPTY_FIELD_VALUE, websiteUrl) ? "" : websiteUrl)
                .build()
                ;
    }
}
