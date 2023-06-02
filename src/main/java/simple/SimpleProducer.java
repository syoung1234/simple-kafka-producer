package simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        // 메시지 키 미포함
        String messageValue = "test message";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        producer.send(record);
        logger.info("{}", record);

        // 메시지 키 포함
        ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, "key1", "vvv1");
        producer.send(record2);

        // 파티션 번호를 지정하여 전송
        int partitionNo = 0;
        ProducerRecord<String, String> record3 = new ProducerRecord<>(TOPIC_NAME, partitionNo, "key2", "vvv2");
        producer.send(record3);

        // get() method 를 사용해서 데이터 결과 동기적으로 가져오기
        ProducerRecord<String, String> record4 = new ProducerRecord<>(TOPIC_NAME, "key4", "v4");
        RecordMetadata metadata = producer.send(record4).get();
        logger.info(metadata.toString());


        producer.flush();
        producer.close();
    }
}
