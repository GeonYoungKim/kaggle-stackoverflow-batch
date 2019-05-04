package com.skuniv.cs.geonyeong.kaggle;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class KaggleApplicationTests {
    String time = "2012-04-03 21:24:21.690000+00:00";
    SimpleDateFormat transFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void test() throws ParseException {
        Date to = transFormat.parse(time);
        log.info("to => {}", to);
        log.info("from => {}", transFormat.format(to));
    }



}
