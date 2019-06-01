package com.skuniv.cs.geonyeong.kaggle.batch.migration;

import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.CHUNCK_SIZE;
import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.HIVE_DELEMETER_FIRST;
import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.HIVE_DELEMETER_SECOND;

import com.google.gson.Gson;
import com.skuniv.cs.geonyeong.kaggle.enums.PostType;
import com.skuniv.cs.geonyeong.kaggle.support.BatchSupport;
import com.skuniv.cs.geonyeong.kaggle.utils.BatchUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.TimeUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.Account;
import com.skuniv.cs.geonyeong.kaggle.vo.Comment;
import com.skuniv.cs.geonyeong.kaggle.vo.Link;
import com.skuniv.cs.geonyeong.kaggle.vo.QnaJoin;
import com.skuniv.cs.geonyeong.kaggle.vo.Question;
import java.util.Date;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
public class KaggleMigrationPostQuestionJobConfiguration {
    private final static Gson gson = new Gson();

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index.post}")
    private String esIndexName;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.type}")
    private String esType;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final RestHighLevelClient restHighLevelClient;

    @Bean
    public Job kaggleMigrationQuestionJob() {
        return jobBuilderFactory.get("kaggleMigrationQuestionJob")
            .start(kaggleMigrationQuestionStep())
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
    public MultiResourceItemReader<Question> multiResourceQuestionItemReader(
        @Value("#{jobParameters[questionPath]}") String questionPath) {
        MultiResourceItemReader<Question> multiResourceItemReader = BatchUtil
            .createMultiResourceItemReader(questionPath);
        multiResourceItemReader.setDelegate(questionReader());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<Question> questionReader() {
        FlatFileItemReader<Question> reader = new FlatFileItemReader<Question>();
        reader.setLineMapper((line, lineNumber) -> {
            String[] questionSplit = line.split(HIVE_DELEMETER_FIRST, -1);
            String[] commentSplit = questionSplit[19].split(HIVE_DELEMETER_SECOND, -1);
            String[] linkSplit = questionSplit[20].split(HIVE_DELEMETER_SECOND, -1);
            List<Comment> commentList = BatchSupport.createCommentList(commentSplit);
            List<Link> linkList = BatchSupport.createLinkList(linkSplit);
            Account account = BatchSupport.createAccount(
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
                .title(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[1]) ? ""
                    : new String(Base64.decodeBase64(questionSplit[1].substring(2, questionSplit[1].length()-1))))
                .body(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[2]) ? ""
                    : new String(Base64.decodeBase64(questionSplit[2].substring(2, questionSplit[2].length()-1))))
                .answerCount(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[3]) ? 0
                    : Integer.valueOf(questionSplit[3]))
                .commentCount(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[4]) ? 0
                    : Integer.valueOf(questionSplit[4]))
                .createDate(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[5]) ? TimeUtil
                    .toStr(new Date()) : TimeUtil.toStr(questionSplit[5]))
                .favoriteCount(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[6]) ? 0
                    : Integer.valueOf(questionSplit[6]))
                .score(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[9]) ? 0
                    : Integer.valueOf(questionSplit[9]))
                .tags(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[10]) ? ""
                    : questionSplit[10])
                .viewCount(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, questionSplit[11]) ? 0
                    : Integer.valueOf(questionSplit[11]))
                .account(account)
                .commentList(commentList)
                .linkList(linkList)
                .qnaJoin(QnaJoin.builder().name(PostType.QUESTION.getType()).build())
                .build();
            return question;
        });
        return reader;
    }

    @Bean
    public ItemWriter<Question> itemQuestionWriter(String indexName) {
        ItemWriter<Question> answerItemWriter = items -> {
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
            BulkResponse bulkItemResponses = restHighLevelClient
                .bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkItemResponses.hasFailures()) {
                log.info("question response => {}", bulkItemResponses.buildFailureMessage());
            }
        };
        return answerItemWriter;
    }
}
