package streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CustomTransformerApp {
  private static final String APPLICATION_ID = "custom-transformer-v0.1.0";
  private static final String APPLICATION_NAME = "Custom Transformer App";

  public static void main(String[] args) throws InterruptedException {
    System.out.printf("*** Starting %s Application ***%n", APPLICATION_NAME);

    Properties config = getConfig();
    Topology topology = getTopology();

    final CountDownLatch latch = new CountDownLatch(1);
    try {
      KafkaStreams streams = startApp(config, topology);
      setupShutdownHook(streams, latch);
      latch.await();
    } catch (final Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }

  private static Topology getTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    StoreBuilder <KeyValueStore<String, Long>> storeBuilder =  Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("Counts"), Serdes.String(), Serdes.Long());
    builder.addStateStore(storeBuilder);

    builder.stream("lines-topic", Consumed.with(Serdes.String(), Serdes.String()))
        .flatMapValues(line -> Arrays.asList(line.toLowerCase(Locale.getDefault()).split(" ")))
        .selectKey((k, word) -> word)
        .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
        .transform(WordCountTransformer::new, storeBuilder.name())
        .to("word-count-topic", Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }

  private static Properties getConfig() {
    Properties settings = new Properties();
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return settings;
  }

  private static KafkaStreams startApp(Properties config, Topology topology) {
    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.start();
    return streams;
  }

  private static void setupShutdownHook(KafkaStreams streams, CountDownLatch latch) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.printf("### Stopping %s Application ###%n", APPLICATION_NAME);
                  streams.close();
                  latch.countDown();
                }));
  }
}
