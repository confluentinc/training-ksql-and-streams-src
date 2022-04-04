package clicks;

import clicks.ClicksProtos.ClicksOuterClass.Clicks;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

public class Producer {
  static final String DATA_FILE_PREFIX = "./clicks/";
  static final String KAFKA_TOPIC = "clicks-topic";

  /**
   * Java producer.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    System.out.println("Starting Java producer.");

    // Creating the Kafka producer
    final Properties settings = new Properties();
    settings.put(ProducerConfig.CLIENT_ID_CONFIG, "clicks-producer");
    settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
    settings.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
    settings.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            List.of(MonitoringProducerInterceptor.class));

    final KafkaProducer<String, Clicks> producer = new KafkaProducer<>(settings);

    
    // Adding a shutdown hook to clean up when the application exits
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Closing producer.");
      producer.close();
    }));

    final String[] rows = Files.readAllLines(Paths.get(DATA_FILE_PREFIX + "clicks-data.csv"),
            StandardCharsets.UTF_8).toArray(new String[0]);

    Long instant = Instant.now().toEpochMilli();

    for (int i = 0; i < rows.length - 1; i++) {
      final String line = rows[i];
      final String[] values = line.split(",");
      final Clicks message = Clicks.newBuilder()
              .setIp(values[0])
              .setTimestamp(instant + Long.parseLong(values[1]))
              .setUrl(values[2])
              .build();
      final String key = values[0];

      final ProducerRecord<String, Clicks> record = new ProducerRecord<>(KAFKA_TOPIC, key, message);
      producer.send(record);
      System.out.println("Message sent: " + line);
      Thread.sleep(1000);
    }

    producer.close();
  }
}