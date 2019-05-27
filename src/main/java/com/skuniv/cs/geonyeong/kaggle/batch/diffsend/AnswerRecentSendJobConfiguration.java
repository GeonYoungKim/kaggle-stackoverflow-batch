package com.skuniv.cs.geonyeong.kaggle.batch.diffsend;

import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.CHUNCK_SIZE;
import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.HIVE_DELEMETER_FIRST;
import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.HIVE_DELEMETER_SECOND;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.enums.PostType;
import com.skuniv.cs.geonyeong.kaggle.utils.BatchUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.KafkaProducerFactoryUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.TimeUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAccount;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAnswer;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroComment;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroLink;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroQnaJoin;
import java.util.Date;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
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
public class AnswerRecentSendJobConfiguration extends AbstractRecentConfig {

    private KafkaProducer<String, AvroAnswer> kafkaProducer;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job answerRecentSendJob() {
        return jobBuilderFactory.get("answerRecentSendJob")
            .start(answerRecentSendStep())
            .build()
            ;
    }

    @Bean
    public Step answerRecentSendStep() {
        return stepBuilderFactory.get("answerRecentSendStep")
            .<AvroAnswer, AvroAnswer>chunk(CHUNCK_SIZE)
            .reader(multiResourceAnswerRecentItemReader(null))
            .writer(itemAnswerRecentWriter())
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
    public MultiResourceItemReader<AvroAnswer> multiResourceAnswerRecentItemReader(
        @Value("#{jobParameters[answerRecentPath]}") String answerRecentPath) {
        MultiResourceItemReader<AvroAnswer> multiResourceItemReader = BatchUtil
            .createMultiResourceItemReader(answerRecentPath);
        multiResourceItemReader.setDelegate(answerRecent());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<AvroAnswer> answerRecent() {
        FlatFileItemReader<AvroAnswer> reader = new FlatFileItemReader<AvroAnswer>();
        reader.setLineMapper((line, lineNumber) -> {
            String[] answerSplit = line.split(HIVE_DELEMETER_FIRST, -1);

            List<AvroComment> avroCommentList = createAvroCommentList(
                answerSplit[16].split(HIVE_DELEMETER_SECOND, -1));
            List<AvroLink> avroLinkList = createAvroLinkList(
                answerSplit[17].split(HIVE_DELEMETER_SECOND, -1));
            AvroAccount avroAccount = createAvroAccount(
                answerSplit[7], // id
                answerSplit[8], // name
                answerSplit[9], // aboutMe
                answerSplit[10], // age
                answerSplit[11], // createDate
                answerSplit[12], // upVotes
                answerSplit[13], // downVotes
                answerSplit[14], // profileImageUrl
                answerSplit[15] // websiteUrl
            );

            return AvroAnswer.newBuilder()
                .setId(answerSplit[0])
                .setBody(
                    StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[1]) ? "" : answerSplit[1])
                .setCommentCount(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[2]) ? 0
                    : Integer.valueOf(answerSplit[2]))
                .setCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[3]) ? TimeUtil
                    .toStr(new Date()) : TimeUtil.toStr(answerSplit[3]))
                .setParentId(answerSplit[4])
                .setScore(StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[5]) ? 0
                    : Integer.valueOf(answerSplit[5]))
                .setTags(
                    StringUtils.equals(EMPTY_FIELD_VALUE, answerSplit[6]) ? "" : answerSplit[6])
                .setAccount(avroAccount)
                .setCommentList(avroCommentList)
                .setLinkList(avroLinkList)
                .setQnaJoin(AvroQnaJoin.newBuilder()
                    .setName(PostType.ANSWER.getType())
                    .setParent(answerSplit[4])
                    .build())
                .build();
        });
        return reader;
    }

    @Bean
    public ItemWriter<AvroAnswer> itemAnswerRecentWriter() {
        ItemWriter<AvroAnswer> itemWriter = items -> items.forEach(item -> {
            kafkaProducer.send(
                new ProducerRecord<String, AvroAnswer>(KafkaTopicType.ANSWER_RECENT.name(),
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
