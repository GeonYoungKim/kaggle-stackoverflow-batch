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
public class QuestionRecentSendJobConfiguration extends AbstractRecentConfig {
    private KafkaProducer<String, AvroQuestion> kafkaProducer;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job questionRecentSendJob() throws ConfigurationException {
        return jobBuilderFactory.get("questionRecentSendJob")
                .start(questionRecentSendStep())
                .build()
                ;
    }

    @Bean
    public Step questionRecentSendStep() throws ConfigurationException {
        return stepBuilderFactory.get("questionRecentSendStep")
                .<AvroQuestion, AvroQuestion>chunk(CHUNCK_SIZE)
                .reader(multiResourceQuestionRecentItemReader(null))
                .writer(itemQuestionRecentWriter())
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
                String[] questionsplit = line.split(BatchUtil.HIVE_DELEMETER_FIRST, -1);
                List<AvroComment> avroCommentList = createAvroCommentList(questionsplit[19].split(BatchUtil.HIVE_DELEMETER_SECOND, -1));
                List<AvroLink> avroLinkList = createAvroLinkList(questionsplit[20].split(BatchUtil.HIVE_DELEMETER_SECOND, -1));
                AvroAccount avroAccount = createAvroAccount(
                        questionsplit[10], // id
                        questionsplit[11], // name
                        questionsplit[12], // aboutMe
                        questionsplit[13], // age
                        questionsplit[14], // createDate
                        questionsplit[15], // upVotes
                        questionsplit[16], // downVotes
                        questionsplit[17], // profileImageUrl
                        questionsplit[18] // websiteUrl
                );
                return AvroQuestion.newBuilder()
                        .setId(questionsplit[0])
                        .setTitle(questionsplit[1])
                        .setBody(questionsplit[2])
                        .setAnswerCount(Integer.valueOf(questionsplit[3]))
                        .setCommentCount(Integer.valueOf(questionsplit[4]))
                        .setCreateDate(StringUtils.equals(EMPTY_FIELD_VALUE, questionsplit[5]) ? TimeUtil.toStr(new Date()) : TimeUtil.toStr(questionsplit[5]))
                        .setFavoriteCount(Integer.valueOf(questionsplit[6]))
                        .setScore(Integer.valueOf(questionsplit[7]))
                        .setTags(questionsplit[8])
                        .setViewCount(Integer.valueOf(questionsplit[9]))
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
    public ItemWriter<AvroQuestion> itemQuestionRecentWriter() throws ConfigurationException {
        kafkaProducer = KafkaProducerFactoryUtil.createKafkaProducer();
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
