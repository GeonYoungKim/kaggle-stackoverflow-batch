package com.skuniv.cs.geonyeong.kaggle.batch.diffsend;

import com.skuniv.cs.geonyeong.kaggle.enums.KafkaTopicType;
import com.skuniv.cs.geonyeong.kaggle.utils.BatchUtil;
import com.skuniv.cs.geonyeong.kaggle.utils.KafkaProducerFactoryUtil;
import com.skuniv.cs.geonyeong.kaggle.vo.avro.AvroAnswer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.ex.ConfigurationException;
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

import java.util.List;

import static com.skuniv.cs.geonyeong.kaggle.constant.KaggleBatchConstant.CHUNCK_SIZE;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
@Import({KafkaProducerFactoryUtil.class})
public class AnswerDeleteSendJobConfiguration {
    private KafkaProducer<String, AvroAnswer> kafkaProducer;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job answerDeleteSendJob() {
        return jobBuilderFactory.get("answerDeleteSendJob")
                .start(answerDeleteSendStep())
                .build()
                ;
    }

    @Bean
    public Step answerDeleteSendStep() {
        return stepBuilderFactory.get("answerDeleteSendStep")
                .<AvroAnswer, AvroAnswer>chunk(CHUNCK_SIZE)
                .reader(multiResourceAnswerDeleteItemReader(null))
                .writer(itemAnswerDeleteWriter())
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
    public MultiResourceItemReader<AvroAnswer> multiResourceAnswerDeleteItemReader(@Value("#{jobParameters[answerDeletePath]}") String answerDeletePath) {
        MultiResourceItemReader<AvroAnswer> multiResourceItemReader = BatchUtil.createMultiResourceItemReader(answerDeletePath);
        multiResourceItemReader.setDelegate(answerDelete());
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<AvroAnswer> answerDelete() {
        FlatFileItemReader<AvroAnswer> reader = new FlatFileItemReader<AvroAnswer>();
        reader.setLineMapper(new LineMapper<AvroAnswer>() {
            @Override
            public AvroAnswer mapLine(String line, int lineNumber) throws Exception {
                log.info("line => {}", line);
                return AvroAnswer.newBuilder().setId(line).build();
            }
        });
        return reader;
    }

    @Bean
    public ItemWriter<AvroAnswer> itemAnswerDeleteWriter() {
        ItemWriter<AvroAnswer> itemWriter = new ItemWriter<AvroAnswer>() {
            @Override
            public void write(List<? extends AvroAnswer> items) throws Exception {
                items.forEach(item -> {
                    kafkaProducer.send(new ProducerRecord<String, AvroAnswer>(KafkaTopicType.ANSWER_DELETE.name(), item.getId(), item));
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
