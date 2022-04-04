package streams;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;

public class StreamsApp {
    public static class TemperatureReading{
        public String station;
        public Integer temperature;
    }

    final static String APPLICATION_ID = "streams-app-v0.1.0";
    final static String APPLICATION_NAME = "Kafka Streams App";
    final static String INPUT_TOPIC = "temperature-readings";
    final static String OUTPUT_TOPIC = "max-temperatures";

    public static void main(String[] args) {
        System.out.printf("*** Starting %s Application ***%n", APPLICATION_NAME);

        Properties config = getConfig();
        Topology topology = getTopology();
        KafkaStreams streams =  startApp(config, topology);

        setupShutdownHook(streams);
    }

    private static Properties getConfig(){
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");

        // Interceptor configuration
        settings.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        settings.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        return settings;        
    }

    private static Topology getTopology() {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<TemperatureReading> tempSerde = getJsonSerde();
        final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(INPUT_TOPIC, Consumed.with(stringSerde, tempSerde))
            .mapValues(v -> {
                try { TimeUnit.MILLISECONDS.sleep(300); } catch(Exception ex) {}      // artificially delay execution
                return v;
            })
            .groupByKey(Grouped.with(stringSerde, tempSerde))
            .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
            .reduce((aggValue, newValue) -> newValue.temperature > aggValue.temperature ? newValue : aggValue)
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(windowedStringSerde, tempSerde));
        return builder.build();
    }

    private static Serde<TemperatureReading> getJsonSerde(){
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("json.value.type", TemperatureReading.class);

        final Serializer<TemperatureReading> serializer = new KafkaJsonSerializer<>();
        serializer.configure(serdeProps, false);

        final Deserializer<TemperatureReading> deserializer = new KafkaJsonDeserializer<>();
        deserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(serializer, deserializer);
    }

    private static KafkaStreams startApp(Properties config, Topology topology){
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();
        return streams;
    }

    private static void setupShutdownHook(KafkaStreams streams){
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.printf("### Stopping %s Application ###%n", APPLICATION_NAME);
            streams.close();
        }));
    }
}
