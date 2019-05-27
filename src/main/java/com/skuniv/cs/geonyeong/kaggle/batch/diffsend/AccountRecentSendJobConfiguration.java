package com.skuniv.cs.geonyeong.kaggle.batch.diffsend;

import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.CHUNCK_SIZE;
import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.HIVE_DELEMETER_FIRST;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.utils.BatchUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.KafkaProducerFactoryUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAccount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
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
import org.springframework.context.annotation.Import;

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
    public Job accountRecentSendJob() {
        return jobBuilderFactory.get("accountRecentSendJob")
            .start(accountRecentSendStep())
            .build()
            ;
    }

    @Bean
    public Step accountRecentSendStep() {
        return stepBuilderFactory.get("accountRecentSendStep")
            .<AvroAccount, AvroAccount>chunk(CHUNCK_SIZE)
            .reader(multiResourceAccountRecentItemReader(null))
            .writer(itemAccountRecentWriter())
            .listener(new StepExecutionListener() {
                @Override
                public void beforeStep(StepExecution stepExecution) {
                    try {
                        kafkaProducer = KafkaProducerFactoryUtil.createKafkaProducer();
                    } catch (ConfigurationException e) {
                        log.info("KAFKA PRODUCER CREATE ERROR => {}", e);
                    }
                }

                @Override
                public ExitStatus afterStep(StepExecution stepExecution) {
                    return null;
                }
            })
            .build()
            ;
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<AvroAccount> multiResourceAccountRecentItemReader(
        @Value("#{jobParameters[accountRecentPath]}") String accountRecentPath) {
        MultiResourceItemReader<AvroAccount> multiResourceItemReader = BatchUtil
            .createMultiResourceItemReader(accountRecentPath);
        multiResourceItemReader.setDelegate(accountRecent());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<AvroAccount> accountRecent() {
        FlatFileItemReader<AvroAccount> reader = new FlatFileItemReader<AvroAccount>();
        reader.setLineMapper((line, lineNumber) -> {
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
        });
        return reader;
    }

    @Bean
    public ItemWriter<AvroAccount> itemAccountRecentWriter() {
        ItemWriter<AvroAccount> itemWriter = items -> items.forEach(item -> {
            kafkaProducer.send(
                new ProducerRecord<String, AvroAccount>(KafkaTopicType.ACCOUNT_RECENT.name(),
                    item.getId(), item));
            try {
                Thread.sleep(10l);
            } catch (InterruptedException e) {
                log.error("KAFKA PRODUCE InterruptedException => {}", e);
            }
        });
        return itemWriter;
    }

}
