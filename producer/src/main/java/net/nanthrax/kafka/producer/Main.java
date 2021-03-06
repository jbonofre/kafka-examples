package net.nanthrax.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Main {

    // NB: create the topic on kafka first
    private final static String TOPIC = "my-example-topic";

    private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    private static Producer<Long, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // the batch.size in bytes of record size, 0 to disable batching
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

        // linger how much to wait for other records before sending the batch over the network
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        // the total bytes of memory the producer can use to buffer records waiting to be sent to the Kafka broker.
        // If records are sent faster than broker can handle then the producer blocks. Used for compression and in-flight records.
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);

        // control how much time Producer blocks before throwing BufferExhaustedException
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);

        return new KafkaProducer<Long, String>(properties);
    }

    static void runProducerSync(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, index, "Hello " + index);
                RecordMetadata metadata = producer.send(record).get();
                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) meta(partition=%d offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    static void runProducerASync(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);
        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, index, "Hello " + index);
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) {
                        System.out.printf("sent record(key=%s value=%s) meta(partition=%d offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
            }
            countDownLatch.await(25, TimeUnit.SECONDS);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runProducerSync(5);
        } else if (args.length == 1) {
            runProducerSync(Integer.parseInt(args[0]));
        } else if (args.length == 2) {
            if (args[1].equals("sync")) {
                runProducerSync(Integer.parseInt(args[0]));
            } else {
                runProducerASync(Integer.parseInt(args[0]));
            }
        }
    }

}
