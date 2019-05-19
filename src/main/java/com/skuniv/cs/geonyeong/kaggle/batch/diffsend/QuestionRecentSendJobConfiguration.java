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
import org.springframework.batch.core.*;
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

import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.*;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
@Import({KafkaProducerFactoryUtil.class})
public class QuestionRecentSendJobConfiguration extends AbstractRecentConfig {
    private KafkaProducer<String, AvroQuestion> kafkaProducer;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job questionRecentSendJob() {
        return jobBuilderFactory.get("questionRecentSendJob")
                .start(questionRecentSendStep())
                .build()
                ;
    }

    @Bean
    public Step questionRecentSendStep() {
        return stepBuilderFactory.get("questionRecentSendStep")
                .<AvroQuestion, AvroQuestion>chunk(CHUNCK_SIZE)
                .reader(multiResourceQuestionRecentItemReader(null))
                .writer(itemQuestionRecentWriter())
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
    public MultiResourceItemReader<AvroQuestion> multiResourceQuestionRecentItemReader(@Value("#{jobParameters[questionRecentPath]}") String questionRecentPath) {
        MultiResourceItemReader<AvroQuestion> multiResourceItemReader = BatchUtil.createMultiResourceItemReader(questionRecentPath);
        multiResourceItemReader.setDelegate(questionRecent());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<AvroQuestion> questionRecent() {
        FlatFileItemReader<AvroQuestion> reader = new FlatFileItemReader<AvroQuestion>();
        reader.setLineMapper(new LineMapper<AvroQuestion>() {
            @Override
            public AvroQuestion mapLine(String line, int lineNumber) throws Exception {
                String[] questionSplit = line.split(HIVE_DELEMETER_FIRST, -1);
                List<AvroComment> avroCommentList = createAvroCommentList(questionSplit[19].split(HIVE_DELEMETER_SECOND, -1));
                List<AvroLink> avroLinkList = createAvroLinkList(questionSplit[20].split(HIVE_DELEMETER_SECOND, -1));
                AvroAccount avroAccount = createAvroAccount(
                        questionSplit[10], // id
                        questionSplit[11], // name
                        questionSplit[12], // aboutMe
                        questionSplit[13], // age
                        questionSplit[14], // createDate
                        questionSplit[15], // upVotes
                        questionSplit[16], // downVotes
                        questionSplit[17], // profileImageUrl
                        questionSplit[18] // websiteUrl
                );
                return AvroQuestion.newBuilder()
                        .setId(questionSplit[0])
                        .setTitle(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[1]) ? "" : questionSplit[1])
                        .setBody(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[2]) ? "" : questionSplit[2])
                        .setAnswerCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[3]) ? 0 : Integer.valueOf(questionSplit[3]))
                        .setCommentCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[4]) ? 0 : Integer.valueOf(questionSplit[4]))
                        .setCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[5]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(questionSplit[5]))
                        .setFavoriteCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[6]) ? 0 : Integer.valueOf(questionSplit[6]))
                        .setScore(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[7]) ? 0 : Integer.valueOf(questionSplit[7]))
                        .setTags(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[8]) ? "" : questionSplit[8])
                        .setViewCount(StringUtils.equals(EMPTY_FIELD_VALUE, questionSplit[9]) ? 0 : Integer.valueOf(questionSplit[9]))
                        .setAccount(avroAccount)
                        .setCommentList(avroCommentList)
                        .setLinkList(avroLinkList)
                        .setQnaJoin(AvroQnaJoin.newBuilder()
                                .setName(PostType.QUESTION.getType())
                                .build())
                        .build();
            }
        });
        return reader;
    }

    @Bean
    public ItemWriter<AvroQuestion> itemQuestionRecentWriter() {
        ItemWriter<AvroQuestion> itemWriter = new ItemWriter<AvroQuestion>() {
            @Override
            public void write(List<? extends AvroQuestion> items) throws Exception {
                items.forEach(item -> {
                    kafkaProducer.send(new ProducerRecord<String, AvroQuestion>(KafkaTopicType.QUESTION_RECENT.name(), item.getId(), item));
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
