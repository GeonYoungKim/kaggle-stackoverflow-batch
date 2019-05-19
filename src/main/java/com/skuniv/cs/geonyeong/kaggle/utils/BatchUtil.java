package com.skuniv.cs.geonyeong.kaggle.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.IOException;

@Slf4j
public class BatchUtil {
    public static final String HIVE_DELEMETER_FIRST = "\001";
    public static final String HIVE_DELEMETER_SECOND = "\002";

    public static MultiResourceItemReader createMultiResourceItemReader(String path) {
        ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = null;
        try {
            resources = patternResolver.getResources("file:" + path);
        } catch (IOException e) {
            log.error("resources get error");
        }
        MultiResourceItemReader<String> multiResourceItemReader = new MultiResourceItemReader<String>();
        multiResourceItemReader.setResources(resources);
        return multiResourceItemReader;
    }
}
