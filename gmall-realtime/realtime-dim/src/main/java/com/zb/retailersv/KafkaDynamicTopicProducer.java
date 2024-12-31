package com.zb.retailersv;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.Collections;

public class KafkaDynamicTopicProducer {

    private static final String KAFKA_BROKER = "localhost:9092";

    public static void main(String[] args) {
        // 示例：假设这是从 MySQL CDC 或其他源提取的 JSON 数据
        String jsonData = "{\"source\": {\"table\": \"user_table\"}, \"payload\": {\"id\": 1, \"name\": \"John Doe\"}}";
        JSONObject jsonObject = JSONObject.parseObject(jsonData);

        // 提取表名
        String tableName = jsonObject.getJSONObject("source").getString("table");

        // 检查并创建 Kafka topic
        createTopicIfNotExist(tableName);

        // 发送数据到 Kafka topic
        sendDataToKafka(tableName, jsonObject);
    }

    /**
     * 检查 Kafka 中是否存在指定的 topic，如果不存在则创建
     */
    private static void createTopicIfNotExist(String tableName) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Create topic if not exist
            NewTopic newTopic = new NewTopic(tableName, 1, (short) 1); // 1 partition, 1 replica
            try {
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("Topic created: " + tableName);
            } catch (TopicExistsException e) {
                System.out.println("Topic already exists: " + tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将数据发送到 Kafka topic
     */
    private static void sendDataToKafka(String tableName, JSONObject jsonObject) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", KAFKA_BROKER);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // 将数据发送到对应的 Kafka topic
            ProducerRecord<String, String> record = new ProducerRecord<>(tableName, jsonObject.toJSONString());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("Message sent to topic: " + tableName);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
