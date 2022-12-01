package com.kevin.kafka.csm;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;


/**
 * @author TX
 * @date 2021/1/13 9:40
 */
public class Receiver {

    private Logger logger = LoggerFactory.getLogger(Receiver.class);
    private KafkaConsumer<String, String> consumer;
    private boolean run = false;

    public void start(){
        logger.info("Receiver started.");
        run = true;
        Properties props = new Properties();
        InputStream in = Receiver.class.getClassLoader().getResourceAsStream("kafka.properties");
        try {
            props.load(in);
            consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Arrays.asList("DemoTp2"));
            while (run){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for(ConsumerRecord record: records){
                    logger.debug(String.format("Received: topic=%s, partition=%s, offset=%d, key=%s, value=%s",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()));
                }
            }
        } catch (Exception e) {
            logger.error("异常!", e);
        }
        finally {
            logger.info("Receiver closed.");
            consumer.close();
        }
    }

    public void stop(){
        logger.info("stop Receiver");
        run = false;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        logger.info("finalize: Receiver closed.");
        consumer.close();
    }
}
