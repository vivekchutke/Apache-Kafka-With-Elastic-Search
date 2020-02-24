package com.vivekchutke.explorekafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {


    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        logger.info("Producing a message to Kafka topic");
        // Create producer Properties
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producers = new KafkaProducer<String, String>(properties);

        //Produced Records
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Old now Hello World from Java");
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("first_topic", "Third Message from Java");


        // Send Data - Asyn(Data is never sent
        producers.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e ==null) {
                            logger.info("Topic is:"+recordMetadata.topic());
                            logger.info("Partitions are:"+ recordMetadata.offset());
                            logger.info("Timestamp"+recordMetadata.timestamp() );
                        } else {
                            logger.error("Error While Producing: ", e);
                        }

                    }
                }
        );
        producers.send(record1);

        // Flush the data
        producers.flush();
        producers.close();


    }
}
