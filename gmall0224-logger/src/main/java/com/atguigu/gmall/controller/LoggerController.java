package com.atguigu.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

/**
 * @author: wxw
 * @date: 2021/8/2
 * @desc: 日志数据的采集
 */
@RestController
@Slf4j
public class LoggerController {
@Autowired
    private KafkaTemplate kafkaTemplate;
@RequestMapping(value = "/applog")
    public String log(@RequestParam("param")String logStr) {
//    1打印输出到控制台
//    System.out.println(logStr);
//    2落盘--利用logback
    log.info(logStr);
//    3发送打kafka 主题
//    Properties props = new Properties();
//    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
//    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"");
//    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"");
//
//    KafkaProducer kafkaProducer = new KafkaProducer(props);
//    kafkaProducer.send(
//            new ProducerRecord("ods_base_log",logStr)
//    );
    kafkaTemplate.send("ods_base_log",logStr);
    return "success";
}
}
