package com.skuniv.cs.geonyeong.kaggle.batch.migration;


import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.CHUNCK_SIZE;

import com.google.gson.Gson;
import com.skuniv.cs.geonyeong.kaggle.utils.BatchUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.TimeUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.Account;
import java.util.Date;
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
public class KaggleMigrationAccountJobConfiguration {

    private final static Gson gson = new Gson();

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.index.account}")
    private String esIndex;

    @Value("${com.skuniv.cs.geonyeong.kaggle.es.type}")
    private String esType;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final RestHighLevelClient restHighLevelClient;

    private final String DELEMETER = "`";
    private final String EMPTY_FIELD_VALUE = "None";

    @Bean
    public Job kaggleMigrationAccountJob() {
        return jobBuilderFactory.get("kaggleMigrationAccountJob")
            .start(kaggleMigrationAccountStep())
            .build()
            ;
    }

    @Bean
    public Step kaggleMigrationAccountStep() {
        return stepBuilderFactory.get("kaggleMigrationAccountStep")
            .<Account, Account>chunk(CHUNCK_SIZE)
            .reader(multiResourceAccountItemReader(null))
            .writer(itemAccountWriter(esIndex))
            .build()
            ;
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<Account> multiResourceAccountItemReader(
        @Value("#{jobParameters[accountPath]}") String accountPath) {
        MultiResourceItemReader<Account> multiResourceItemReader = BatchUtil
            .createMultiResourceItemReader(accountPath);
        multiResourceItemReader.setDelegate(accountReader());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<Account> accountReader() {
        FlatFileItemReader<Account> reader = new FlatFileItemReader<Account>();
        reader.setLineMapper((line, lineNumber) -> {
            String[] accountSplit = line.split(DELEMETER, -1);
            Account account = Account.builder()
                .id(StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[0]) ? "" : accountSplit[0])
                .displayName(
                    StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[1]) ? "" : new String(Base64.decodeBase64(accountSplit[1].substring(2, accountSplit[1].length()-1))))
                .aboutMe(
                    StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[2]) ? "" : new String(Base64.decodeBase64(accountSplit[2].substring(2, accountSplit[2].length()-1))))
                .age(StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[3]) ? "" : accountSplit[3])
                .createDate(StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[4]) ? TimeUtil
                    .toStr(new Date()) : TimeUtil.toStr(accountSplit[4]))
                .upvotes(StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[5]) ? 0
                    : Integer.valueOf(accountSplit[5]))
                .downVotes(StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[6]) ? 0
                    : Integer.valueOf(accountSplit[6]))
                .profileImageUrl(
                    StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[7]) ? "" : accountSplit[7])
                .websiteUrl(
                    StringUtils.equals(EMPTY_FIELD_VALUE, accountSplit[8]) ? "" : accountSplit[8])
                .build();

            return account;
        });
        return reader;
    }

    @Bean
    public ItemWriter<Account> itemAccountWriter(String indexName) {
        ItemWriter<Account> answerItemWriter = items -> {
            BulkRequest bulkRequest = new BulkRequest();
            items.forEach(item -> {
                    bulkRequest.add(
                        new IndexRequest(indexName)
                            .source(gson.toJson(item), XContentType.JSON)
                            .type(esType)
                            .id(String.valueOf(item.getId()))
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
