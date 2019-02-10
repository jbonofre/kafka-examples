package net.nanthrax.kafka.examples.endtoend.writer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Main {

    public static void main(String ... args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: ./writer directory");
            System.exit(-1);
        }

        File directory = new File(args[0]);
        directory.mkdirs();

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "writer");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList("endtoend"));
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMinutes(1));
                if (records != null) {
                    for (ConsumerRecord<Long, String> record : records) {
                        File file = new File(directory, record.key().toString());
                        try (PrintWriter printWriter = new PrintWriter(new FileWriter(file))) {
                            printWriter.println(record.value());
                        }
                    }
                }
            }
        }

    }

}
