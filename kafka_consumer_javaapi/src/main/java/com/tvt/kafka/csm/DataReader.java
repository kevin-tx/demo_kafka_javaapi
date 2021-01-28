package com.tvt.kafka.csm;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;


/**
 * @author TX
 * @date 2021/1/13 9:40
 * run: java -jar kfcm.jar nat-proxy-1 1611823202000 192.168.0.51:9092,192.168.0.52:9092,192.168.0.53:9092
 */
public class DataReader {

    private static Logger logger = LoggerFactory.getLogger(DataReader.class);
    private static KafkaConsumer<String, String> consumer;
    public static void main(String[] args) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date date = new Date(2021-1900, 0, 28, 15, 30, 58);
//        System.out.println(df.format(date));
//        System.out.println(new Date(2021-1900, 0, 26, 15, 30, 58).getTime());
//        start(new Date(2021-1900, 0, 26, 15, 30, 58).getTime());
//        System.out.println(df.format(new Date(1611646258000L)));

        if(args.length<1){
            logger.error("---------------------- please input topic");
            return;
        }

        if(args.length<2){
            logger.error("---------------------- please input fetch timestamp");
            return;
        }

        String topic = args[0];
        logger.info("---------------------- topic is: " + topic);

        Long fetchTimestamp = Long.parseLong(args[1]);
        logger.info("---------------------- fetch timestamp is: " + fetchTimestamp + " (" + df.format(new Date(fetchTimestamp)) + ")");
        String bootstrapServer = null;
        if (args.length > 2) {
            bootstrapServer = args[2];
        }
        start(topic, fetchTimestamp, bootstrapServer);
    }

    //从指定时间开始消费
    public static void start(String topic, long fetchTimestamp, String bootstrapServer){
        logger.info("Data Reader started.");
        Properties props = new Properties();
        InputStream in = DataReader.class.getClassLoader().getResourceAsStream("kafka.properties");
        try {
            props.load(in);
            if(bootstrapServer!=null){
                props.put("bootstrap.servers", bootstrapServer);
            }

            logger.info("---------------------- bootstrap.servers is: " + props.getProperty("bootstrap.servers"));
            consumer = new KafkaConsumer<String, String>(props);
            Map<TopicPartition, Long> mapTime = new HashMap<>();
            List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);

            for (PartitionInfo par : partitionInfoList) {
                mapTime.put(new TopicPartition(topic, par.partition()), fetchTimestamp);
            }
            Map<TopicPartition, OffsetAndTimestamp> partitionOffsetTimeMap = consumer.offsetsForTimes(mapTime);
            List<TopicPartition> assignTopicList = new ArrayList<>();
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : partitionOffsetTimeMap.entrySet()) {
                TopicPartition key = entry.getKey();
                OffsetAndTimestamp value = entry.getValue();
                //根据消费里的timestamp确定offset
                if (value != null) {
                    long offset = value.offset();
                    assignTopicList.add(key);
                }
            }

            consumer.assign(assignTopicList);
            for(TopicPartition topicPartition: assignTopicList){
                consumer.seek(topicPartition, partitionOffsetTimeMap.get(topicPartition).offset());
            }

            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));
                for(ConsumerRecord record: records){
                    logger.debug(String.format("[%s]%s--topic=%s, partition=%s, offset=%d",
                            record.key(),
                            record.value(),
                            record.topic(),
                            record.partition(),
                            record.offset()));
                }
            }
        } catch (Exception e) {
            logger.error("异常!", e);
        }
        finally {
            logger.info("Data Reader closed.");
            consumer.close();
        }
    }
}
