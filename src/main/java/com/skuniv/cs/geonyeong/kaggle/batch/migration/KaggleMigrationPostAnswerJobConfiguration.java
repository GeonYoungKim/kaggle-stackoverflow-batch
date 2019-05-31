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
import com.skuniv.cs.geonyeong.kaggle.vo.Answer;
import com.skuniv.cs.geonyeong.kaggle.vo.Comment;
import com.skuniv.cs.geonyeong.kaggle.vo.Link;
import com.skuniv.cs.geonyeong.kaggle.vo.QnaJoin;
import java.util.Date;
import java.util.List;
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
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
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
public class KaggleMigrationPostAnswerJobConfiguration extends DefaultBatchConfigurer {

    private final static Gson gson = new Gson();

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index.post}")
    private String esIndexName;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.type}")
    private String esType;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final RestHighLevelClient restHighLevelClient;

    @Bean
    public Job kaggleMigrationAnswerJob() {
        return jobBuilderFactory.get("kaggleMigrationAnswerJob")
            .start(kaggleMigrationAnswerStep())
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
    @StepScope
    public MultiResourceItemReader<Answer> multiResourceAnswerItemReader(
        @Value("#{jobParameters[answerPath]}") String answerPath) {
        MultiResourceItemReader<Answer> multiResourceItemReader = BatchUtil
            .createMultiResourceItemReader(answerPath);
        multiResourceItemReader.setDelegate(answerReader());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<Answer> answerReader() {
        FlatFileItemReader<Answer> reader = new FlatFileItemReader<Answer>();
        reader.setLineMapper((line, lineNumber) -> {
            String[] answerSplit = line.split(HIVE_DELEMETER_FIRST, -1);
            String[] commentSplit = answerSplit[16].split(HIVE_DELEMETER_SECOND, -1);
            String[] linkSplit = answerSplit[17].split(HIVE_DELEMETER_SECOND, -1);
            List<Comment> commentList = BatchSupport.createCommentList(commentSplit);
            List<Link> linkList = BatchSupport.createLinkList(linkSplit);
            Account account = BatchSupport.createAccount(
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
                .body(
                    StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, answerSplit[1]) ? ""
                        : answerSplit[1])
                .commentCount(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, answerSplit[2]) ? 0
                    : Integer.valueOf(answerSplit[2]))
                .createDate(
                    StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, answerSplit[3]) ? TimeUtil
                        .toStr(new Date()) : TimeUtil.toStr(answerSplit[3]))
                .parentId(
                    StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, answerSplit[6]) ? ""
                        : answerSplit[6])
                .score(StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, answerSplit[7]) ? 0
                    : Integer.valueOf(answerSplit[7]))
                .tags(
                    StringUtils.equals(BatchSupport.EMPTY_FIELD_VALUE, answerSplit[8]) ? ""
                        : answerSplit[8])
                .account(account)
                .commentList(commentList)
                .linkList(linkList)
                .qnaJoin(
                    QnaJoin.builder()
                        .name(PostType.ANSWER.getType())
                        .parent(answerSplit[6])
                        .build()
                )
                .build();
            return answer;
        });
        return reader;
    }

    @Bean
    public ItemWriter<Answer> itemAnswerWriter(String indexName) {
        ItemWriter<Answer> answerItemWriter = items -> {
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
            BulkResponse bulkItemResponses = restHighLevelClient
                .bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkItemResponses.hasFailures()) {
                log.info("answer response => {}", bulkItemResponses.buildFailureMessage());
            }
        };
        return answerItemWriter;
    }
}
