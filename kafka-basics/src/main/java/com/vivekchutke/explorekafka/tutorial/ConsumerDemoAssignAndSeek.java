package com.vivekchutke.explorekafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);
        String bootStrapServer = "localhost:9092";
        String groupId = "my-seventh-application";
        String topic = "first_topic";

        //Consumer Config Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //possible values earliest/latest and none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // Assign and Seek are mostly used to replat data or fetch messages from a particular partition and offset
        // Assign
        TopicPartition partitionsToReadFrom = new TopicPartition(topic, 0);
        long offSetToReadFrom = 5L;

        consumer.assign(Arrays.asList(partitionsToReadFrom));
        consumer.seek(partitionsToReadFrom, offSetToReadFrom);

        //Subscribe consumer to our topip(s)
//          consumer.subscribe(Arrays.asList(topic));

        // If you want to read say five message
        int noOfMessagesToRead = 5;
        int counter = 0;
        // Poll for new data
        while(true) {
            // consumer.poll(100);   # depreceted method hence use duration object
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));    // Duration polling is new to Kafka
            for(ConsumerRecord<String,String> record: records ) {
                counter++;
                logger.info("Key:" +record.key() + ", Values: "+record.value());
                logger.info("Partitions: " + record.partition() +", Offset:"+record.offset());
                if(noOfMessagesToRead < counter) {
                    break;
                }
            }
        }
//        logger.info("Existing after reading from: "+offSetToReadFrom +" to: "+noOfMessagesToRead);
    }
}
