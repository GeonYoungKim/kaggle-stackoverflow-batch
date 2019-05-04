package com.skuniv.cs.geonyeong.kaggle.migration;

import com.google.gson.Gson;
import com.skuniv.cs.geonyeong.kaggle.configuration.EsConfiguration;
import com.skuniv.cs.geonyeong.kaggle.model.vo.*;
import com.skuniv.cs.geonyeong.kaggle.utils.TimeUtil;
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
                log.info("question line => {}", line);
                String[] questionSplit = line.split(HIVE_DELEMETER_FIRST, -1);
                String[] commentSplit = questionSplit[19].split(HIVE_DELEMETER_SECOND, -1);
                String[] linkSplit = questionSplit[20].split(HIVE_DELEMETER_SECOND, -1);
                List<Comment> commentList = createCommentList(commentSplit);
                List<Link> linkList = createLinkList(linkSplit);
                Question question = Question.builder()
                        .questionId(Integer.valueOf(questionSplit[0]))
                        .title(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[1]) ? "" : questionSplit[1])
                        .body(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[2]) ? "" : questionSplit[2])
                        .answerCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[3]) ? 0 : Integer.valueOf(questionSplit[3]))
                        .commentCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[4]) ? 0 : Integer.valueOf(questionSplit[4]))
                        .createDate(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[5]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(questionSplit[5]))
                        .favoriteCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[6]) ? 0 : Integer.valueOf(questionSplit[6]))
                        .ownerDisplayName(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[7]) ? "" : questionSplit[7])
                        .ownerUserId(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[8]) ? 0 : Integer.valueOf(questionSplit[8]))
                        .score(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[9]) ? 0 : Integer.valueOf(questionSplit[9]))
                        .tags(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[10]) ? "" : questionSplit[10])
                        .viewCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[11]) ? 0 : Integer.valueOf(questionSplit[11]))
                        .userAboutMe(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[12]) ? "" : questionSplit[12])
                        .userAge(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[13]) ? "" : questionSplit[13])
                        .userCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[14]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(questionSplit[14]))
                        .userUpVotes(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[15]) ? 0 : Integer.valueOf(questionSplit[15]))
                        .userDownVotes(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[16]) ? 0 : Integer.valueOf(questionSplit[16]))
                        .userProfileImageUrl(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[17]) ? "" : questionSplit[17])
                        .userWebsiteUrl(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[18]) ? "" : questionSplit[18])
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
                Answer answer = Answer.builder()
                        .answerId(Integer.valueOf(answerSplit[0]))
                        .body(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[1]) ? "" : answerSplit[1])
                        .commentCount(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[2]) ? 0 : Integer.valueOf(answerSplit[2]))
                        .createDate(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[3]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(answerSplit[3]))
                        .ownerDisplayName(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[4]) ? "" : answerSplit[4])
                        .ownerUserId(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[5]) ? 0 : Integer.valueOf(answerSplit[5]))
                        .parentId(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[6]) ? 0 : Integer.valueOf(answerSplit[6]))
                        .score(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[7]) ? 0 : Integer.valueOf(answerSplit[7]))
                        .tags(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[8]) ? "" : answerSplit[8])
                        .userAboutMe(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[9]) ? "" : answerSplit[9])
                        .userAge(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[10]) ? "" : answerSplit[10])
                        .userCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[11]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(answerSplit[11]))
                        .userUpVotes(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[12]) ? 0 : Integer.valueOf(answerSplit[12]))
                        .userDownVotes(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[13]) ? 0 : Integer.valueOf(answerSplit[13]))
                        .userProfileImageUrl(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[14]) ? "" : answerSplit[14])
                        .userWebsiteUrl(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[15]) ? "" : answerSplit[15])
                        .commentList(commentList)
                        .linkList(linkList)
                        .qnaJoin(
                                QnaJoin.builder()
                                        .name(ANSWER_JOIN_NAME)
                                        .parent(Integer.valueOf(answerSplit[6]))
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
                                            .id(String.valueOf(item.getAnswerId()))
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
                                            .id(String.valueOf(item.getQuestionId()))
                                            .routing(String.valueOf(item.getQuestionId()))
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
                            .linkId(Integer.valueOf(linkDetailSplit[0]))
                            .postId(Integer.valueOf(linkDetailSplit[1]))
                            .relatedPostId(Integer.valueOf(linkDetailSplit[2]))
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
                        comment = Comment.builder()
                                .commentId(Integer.valueOf(commentDetailSplit[0]))
                                .text(commentDetailSplit[1])
                                .createDate(TimeUtil.toStr(commentDetailSplit[2]))
                                .postId(Integer.valueOf(commentDetailSplit[3]))
                                .userId(Integer.valueOf(commentDetailSplit[4]))
                                .userDisplayName(commentDetailSplit[5])
                                .score(Integer.valueOf(commentDetailSplit[6]))
                                .userAboutMe(commentDetailSplit[7])
                                .userAge(commentDetailSplit[8])
                                .userCreateDate(TimeUtil.toStr(commentDetailSplit[9]))
                                .userUpvotes(Integer.valueOf(commentDetailSplit[10]))
                                .userDownVotes(Integer.valueOf(commentDetailSplit[11]))
                                .userProfileImageUrl(commentDetailSplit[12])
                                .userWebsiteUrl(commentDetailSplit[13])
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
}
