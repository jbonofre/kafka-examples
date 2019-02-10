package net.nanthrax.kafka.examples.endtoend.reader;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class Main {

    public static void main(String ... args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: ./reader file");
            System.exit(-1);
        }

        File file = new File(args[0]);
        if (!file.exists()) {
            System.err.println("File " + args[0] + " doesn't exist");
            System.exit(-2);
        }

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "reader");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<Long, String> producer = new KafkaProducer<>(config)) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            long index = 0;
            while ((line = reader.readLine()) != null) {
                ProducerRecord<Long, String> record = new ProducerRecord<>("endtoend", index, line);
                producer.send(record);
                index++;
            }

            producer.flush();
        }
    }

}
