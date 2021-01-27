package com.tvt.kafka.pro.controller;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * @author TX
 * @date 2021/1/12 20:09
 */
@RestController
public class Sender {

    private Logger logger = LoggerFactory.getLogger(Sender.class);
    @Value("${bootstrap.servers}")
    private String bootstrap_servers;
    @Value("${key.serializer}")
    private String key_serializer;
    @Value("${value.serializer}")
    private String value_serializer;

    private KafkaProducer<String,String> producer;

    @PostConstruct
    private void init(){
        Properties props = new Properties();
        props.put("bootstrap.servers",bootstrap_servers);
        props.put("key.serializer",key_serializer);
        props.put("value.serializer",value_serializer);
        producer = new KafkaProducer<>(props);
    }

    @RequestMapping(value = "/send", method = RequestMethod.GET)
    public String bindDevice(String key, String value) {
        producer.send(new ProducerRecord<String, String>("DemoTp2", key, value));
        String msg = "[" + key + "] " + value;
        logger.debug("Send: " + msg);
        return msg;
    }
}
