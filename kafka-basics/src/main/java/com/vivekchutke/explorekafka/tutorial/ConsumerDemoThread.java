package com.vivekchutke.explorekafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class);
        String bootStrapServer = "localhost:9092";
        String groupId = "my-sixt-application";
        String topic = "first_topic";

        //Consumer Config Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //possible values earliest/latest and none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe consumer to our topip(s)
          consumer.subscribe(Arrays.asList(topic));

        // Poll for new data
        while(true) {
            // consumer.poll(100);   # depreceted method hence use duration object
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));    // Duration polling is new to Kafka
            for(ConsumerRecord<String,String> record: records) {
                logger.info("Key:" +record.key() + ", Values: "+record.value());
                logger.info("Partitions: " + record.partition() +", Offset:"+record.offset());
            }
        }

    }

    public class ConsumeThread implements Runnable {

        private CountDownLatch latch;

        public ConsumeThread(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {

        }

        public void shutdown() {

        }
    }

}
