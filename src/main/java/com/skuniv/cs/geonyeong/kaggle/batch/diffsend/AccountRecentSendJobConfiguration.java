package com.skuniv.cs.geonyeong.kaggle.batch.diffsend;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.utils.BatchUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.KafkaProducerFactoryUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAccount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.List;

import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.CHUNCK_SIZE;
import static com.skuniv.cs.geonyeong.kaggle.utils.BatchUtil.HIVE_DELEMETER_FIRST;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
@Import({KafkaProducerFactoryUtil.class})
public class AccountRecentSendJobConfiguration extends AbstractRecentConfig {
    private KafkaProducer<String, AvroAccount> kafkaProducer;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job accountRecentSendJob() throws ConfigurationException {
        return jobBuilderFactory.get("accountRecentSendJob")
                .start(accountRecentSendStep())
                .build()
                ;
    }

    @Bean
    public Step accountRecentSendStep() throws ConfigurationException {
        return stepBuilderFactory.get("accountRecentSendStep")
                .<AvroAccount, AvroAccount>chunk(CHUNCK_SIZE)
                .reader(multiResourceAccountRecentItemReader(null))
                .writer(itemAccountRecentWriter())
                .build()
                ;
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<AvroAccount> multiResourceAccountRecentItemReader(@Value("#{jobParameters[accountRecentPath]}") String accountRecentPath) {
        MultiResourceItemReader<AvroAccount> multiResourceItemReader = BatchUtil.createMultiResourceItemReader(accountRecentPath);
        multiResourceItemReader.setDelegate(accountRecent());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<AvroAccount> accountRecent() {
        FlatFileItemReader<AvroAccount> reader = new FlatFileItemReader<AvroAccount>();
        reader.setLineMapper(new LineMapper<AvroAccount>() {
            @Override
            public AvroAccount mapLine(String line, int lineNumber) throws Exception {
                String[] accountSplit = line.split(HIVE_DELEMETER_FIRST, -1);
                return createAvroAccount(
                        accountSplit[0], // id
                        accountSplit[1], // name
                        accountSplit[2], // aboutMe
                        accountSplit[3], // age
                        accountSplit[4], // createDate
                        accountSplit[5], // upVotes
                        accountSplit[6], // downVotes
                        accountSplit[7], // profileImageUrl
                        accountSplit[8] // webSiteUrl
                );
            }
        });
        return reader;
    }

    @Bean
    public ItemWriter<AvroAccount> itemAccountRecentWriter() throws ConfigurationException {
        kafkaProducer = KafkaProducerFactoryUtil.createKafkaProducer();
        ItemWriter<AvroAccount> itemWriter = new ItemWriter<AvroAccount>() {
            @Override
            public void write(List<? extends AvroAccount> items) throws Exception {
                items.forEach(item -> {
                    kafkaProducer.send(new ProducerRecord<String, AvroAccount>(KafkaTopicType.ACCOUNT_RECENT.name(), item.getId(), item));
                    try {
                        Thread.sleep(10l);
                    } catch (InterruptedException e) {
                        log.error("KAFKA PRODUCE InterruptedException => {}", e);
                    }
                });
            }
        };
        return itemWriter;
    }

}
