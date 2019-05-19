package com.skuniv.cs.geonyeong.kaggle.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.ConfigurationConverter;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.ClasspathLocationStrategy;
import org.yaml.snakeyaml.Yaml;
import org.apache.commons.configuration2.YAMLConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class YmlUtil {

    public static Properties getYmlProps() throws ConfigurationException {
        YAMLConfiguration yamlConfiguration = getYamlConfig("application.yml");
        Properties properties = ConfigurationConverter.getProperties(yamlConfiguration);
        return properties;
    }

    private static YAMLConfiguration getYamlConfig(String filePath) throws ConfigurationException {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<YAMLConfiguration> builder = new FileBasedConfigurationBuilder<>(YAMLConfiguration.class)
                .configure(params
                        .fileBased()
                        .setFileName(filePath)
                        .setEncoding("UTF-8").setLocationStrategy(new ClasspathLocationStrategy())
                );
        return builder.getConfiguration();
    }
}
