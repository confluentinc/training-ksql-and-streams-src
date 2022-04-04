package io.confluent.training.app;

import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import io.confluent.training.proto.ClicksProtos;
import io.confluent.training.proto.ClicksCountProtos;


import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import com.google.protobuf.StringValue;
import com.google.protobuf.Int64Value;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;

import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;


public class StreamsApp {

    /**
     * Our first streams app.
     */
    public static void main(String[] args) {

        System.out.println(">>> Starting the streams-app Application");

        final Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-clicks-app");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        settings.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ClickEventTimestampExtractor.class);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Topology topology = getTopology();
        // you can paste the topology into this site for a vizualization: https://zz85.github.io/kafka-streams-viz/
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, settings);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("<<< Stopping the streams-clicks-app Application");
            streams.close();
            latch.countDown();
        }));

        // don't do this in prod as it clears your state stores
        streams.cleanUp();
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Topology getTopology() {
        // When you want to override serdes explicitly/selectively
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://schema-registry:8081");
        final Serde<ClicksProtos.Clicks> clicksSerde = new KafkaProtobufSerde<ClicksProtos.Clicks>();
        clicksSerde.configure(serdeConfig, false);
        final Serde<ClicksCountProtos.ClicksCount> clicksCountSerde = new KafkaProtobufSerde<ClicksCountProtos.ClicksCount>();
        clicksCountSerde.configure(serdeConfig, false);



        final StreamsBuilder builder = new StreamsBuilder();

        // TO-DO 1: create a KStream from the "clicks-topic" topic
        //        configure the Key-Serde and Value-Serde that can read the string key, and protobuf-Clicks value
        final KStream<String, ClicksProtos.Clicks> clicks = {{ WRITE-MISSING-CODE }};

        // TO-DO 2: group by key the KStream "clicks"
        final KGroupedStream<String, ClicksProtos.Clicks> clicksGrouped = {{ WRITE-MISSING-CODE }};

        // TO-DO 3: apply a Session Window of 5 minutes with a Grace period of 30 seconds to the KGroupedStream "clicksGrouped"
        //        and apply a count to get the number of clicks per IP and per Session Window
        final KTable<Windowed<String>, Long> clicksCount = {{ WRITE-MISSING-CODE }};

        // TO-DO 4: convert the KTable "clicksCount" into a KStream
        final KStream<Windowed<String>, Long> clicksCountStream = {{ WRITE-MISSING-CODE }};

        // modify the KStream "clicksCountStream" to output a Protobuf Value with info about Session_Start, Session_End, Count and Session_Length
        final KStream<String, ClicksCountProtos.ClicksCount> clicksCountStreamModified = clicksCountStream.map((windowedKey, count) ->  {
            ClicksCountProtos.ClicksCount messageCount = ClicksCountProtos.ClicksCount.newBuilder()
                    .setSessionStartTs(StringValue.of(timeFormatter.format(windowedKey.window().startTime())))
                    .setSessionEndTs(StringValue.of(timeFormatter.format(windowedKey.window().endTime())))
                    .setClickCount(Int64Value.of(count))
                    .setSessionLengthMs(Int64Value.of(windowedKey.window().endTime().toEpochMilli() - windowedKey.window().startTime().toEpochMilli()))
                    .build();
            return KeyValue.pair(windowedKey.key(), messageCount);
        });
            

        // TO-DO 5: produce the data of the KStream "clicksCountStreamModified" to the topic "window-streams" 
        //        selecting the appropiate Serdes for key and value
        {{ WRITE-MISSING-CODE }};

        final Topology topology = builder.build();
        return topology;
    }

    private final static DateTimeFormatter timeFormatter = DateTimeFormatter.ofLocalizedTime(FormatStyle.LONG)
            .withLocale(Locale.US)
            .withZone(ZoneId.systemDefault());

}

