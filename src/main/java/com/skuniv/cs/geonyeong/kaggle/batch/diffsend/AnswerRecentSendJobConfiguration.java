package com.skuniv.cs.geonyeong.kaggle.batch.diffsend;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.enums.PostType;
import com.skuniv.cs.geonyeong.kaggle.utils.BatchUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.KafkaProducerFactoryUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.TimeUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
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

import java.util.Date;
import java.util.List;

import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.CHUNCK_SIZE;

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
    public Job answerRecentSendJob() throws ConfigurationException {
        return jobBuilderFactory.get("answerRecentSendJob")
                .start(answerRecentSendStep())
                .build()
                ;
    }

    @Bean
    public Step answerRecentSendStep() throws ConfigurationException {
        return stepBuilderFactory.get("answerRecentSendStep")
                .<AvroAnswer, AvroAnswer>chunk(CHUNCK_SIZE)
                .reader(multiResourceAnswerRecentItemReader(null))
                .writer(itemAnswerRecentWriter())
                .build()
                ;
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<AvroAnswer> multiResourceAnswerRecentItemReader(@Value("#{jobParameters[answerRecentPath]}") String answerRecentPath) {
        MultiResourceItemReader<AvroAnswer> multiResourceItemReader = BatchUtil.createMultiResourceItemReader(answerRecentPath);
        multiResourceItemReader.setDelegate(answerRecent());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<AvroAnswer> answerRecent() {
        FlatFileItemReader<AvroAnswer> reader = new FlatFileItemReader<AvroAnswer>();
        reader.setLineMapper(new LineMapper<AvroAnswer>() {
            @Override
            public AvroAnswer mapLine(String line, int lineNumber) throws Exception {
                String[] answersplit = line.split(BatchUtil.HIVE_DELEMETER_FIRST, -1);
                List<AvroComment> avroCommentList = createAvroCommentList(answersplit[16].split(BatchUtil.HIVE_DELEMETER_SECOND, -1));
                List<AvroLink> avroLinkList = createAvroLinkList(answersplit[17].split(BatchUtil.HIVE_DELEMETER_SECOND, -1));
                AvroAccount avroAccount = createAvroAccount(
                        answersplit[7], // id
                        answersplit[8], // name
                        answersplit[9], // aboutMe
                        answersplit[10], // age
                        answersplit[11], // createDate
                        answersplit[12], // upVotes
                        answersplit[13], // downVotes
                        answersplit[14], // profileImageUrl
                        answersplit[15] // websiteUrl
                );
                return AvroAnswer.newBuilder()
                        .setId(answersplit[0])
                        .setBody(answersplit[1])
                        .setCommentCount(Integer.valueOf(answersplit[2]))
                        .setCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, answersplit[3])? TimeUtil.toStr(new Date()) :TimeUtil.toStr(answersplit[3]))
                        .setParentId(answersplit[4])
                        .setScore(Integer.valueOf(answersplit[5]))
                        .setTags(answersplit[6])
                        .setAccount(avroAccount)
                        .setCommentList(avroCommentList)
                        .setLinkList(avroLinkList)
                        .setQnaJoin(AvroQnaJoin.newBuilder()
                                .setName(PostType.ANSWER.getType())
                                .setParent(answersplit[4])
                                .build())
                        .build();
            }
        });
        return reader;
    }

    @Bean
    public ItemWriter<AvroAnswer> itemAnswerRecentWriter() throws ConfigurationException {
        kafkaProducer = KafkaProducerFactoryUtil.createKafkaProducer();
        ItemWriter<AvroAnswer> itemWriter = new ItemWriter<AvroAnswer>() {
            @Override
            public void write(List<? extends AvroAnswer> items) throws Exception {
                items.forEach(item -> {
                    kafkaProducer.send(new ProducerRecord<String, AvroAnswer>(KafkaTopicType.ANSWER_RECENT.name(), item.getId(), item));
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
