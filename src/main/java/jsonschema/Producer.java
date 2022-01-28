package jsonschema;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        //For Schema Registry
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        properties.setProperty(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081, http://localhost:8082");
        properties.setProperty(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, "false");

        final String topic = "json-schema-topic";

        KafkaProducer<Integer, User> producer = new KafkaProducer<>(properties);

        try {
            for (int i = 0; i < 100; i++) {
                User user = new User("kei" + i, "su", (short) (i+30));
                ProducerRecord<Integer, User> record = new ProducerRecord<>(topic, i, user);
                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        System.out.println("Success!" );
                        System.out.println(recordMetadata.toString());
                    } else {
                        e.printStackTrace();
                    }
                });
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
