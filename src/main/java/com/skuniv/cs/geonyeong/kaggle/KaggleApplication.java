package com.skuniv.cs.geonyeong.kaggle;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class KaggleApplication implements ApplicationRunner {

    @Autowired
    private ConfigurableApplicationContext configurableApplicationContext;
    @Autowired
    private JobLauncher jobLauncher;

    public static void main(String[] args) {
        SpringApplication.run(KaggleApplication.class, args);
        System.exit(0);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<String> job = args.getOptionValues("job");
        if(Optional.ofNullable(job).isPresent()){
            List<String> param = args.getOptionValues("param");
            log.info("job => " + job.get(0));
            log.info("ctx => " + configurableApplicationContext);
            Job executeJob = (Job) configurableApplicationContext.getBean(job.get(0));
            log.info("executeJob => {}", executeJob.getName());
            JobParametersBuilder builder = new JobParametersBuilder();
            if(Optional.ofNullable(param).isPresent()) {
                String[] paramSplit = param.get(0).split("&", -1);
                Arrays.stream(paramSplit).forEach(item -> {
                    String[] pair = item.split(":");
                    log.info("pair key => {} / value => {}", pair[0], pair[1]);
                    builder.addString(pair[0], pair[1]);
                });
            }
            jobLauncher.run(executeJob, builder.toJobParameters());
        }

    }
}
