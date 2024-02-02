package v2.basic.crud.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import config.CONFIG;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaCrudApp {
    public static void main(String[] args) {

        Gson gson = new GsonBuilder().create();

        //1. kafka consumer property 생성
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CONFIG.KAFKA_SERVER_URL);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONFIG.KAFKA_GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //2. kafka consumer 생성
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        //3. kafka topic 구독
        List<String> topics = new ArrayList<>();
        topics.add("INFLUX_TEST_DEVICE_RECORDS");
        consumer.subscribe(topics);

        //4. consume
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            System.out.println("==================" + records.count() + "==================");
            for (ConsumerRecord<String, String> record : records){
                Map<String, String> keys = gson.fromJson(record.key(), Map.class);
                Map<String, Object> values = gson.fromJson(record.value(), Map.class);
                Map<String, String> attributes = gson.fromJson(values.get("attributes").toString(), Map.class);

                String measurement = Integer.parseInt(String.valueOf(values.get("svcTgtSeq"))) + "_" + values.get("spotDevSeq");
                String tag = keys.get("type");
                String f1 = attributes.get(CONFIG.FIELD1);
                String f2 = attributes.get(CONFIG.FIELD2);
                String f3 = attributes.get(CONFIG.FIELD3);
                String f4 = attributes.get(CONFIG.FIELD4);
                String f5 = attributes.get(CONFIG.FIELD5);

                System.out.println("measurement : " + measurement);
                System.out.println("tag : " + tag);
                System.out.println("f1 : " + f1 + ", f2 : " + f2 + ", f3 : " + f3 + ", f4 : " + f4 + ", f5 : " + f5);
            }
        }
    }
}
