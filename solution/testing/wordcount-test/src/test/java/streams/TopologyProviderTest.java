package streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder; 

public class TopologyProviderTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String inputTopic = "lines-topic";
    private static final String outputTopic = "word-count-topic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(inputTopic);
        CLUSTER.createTopic(outputTopic);
    }

    @Test
    public void shouldCountWords() throws Exception {
        // Step 1: Get Kafka Streams application configuration
        Properties streamsConfiguration = getStreamsConfiguration();

        // Step 2: Get the Kafka Streams application topology.
        TopologyProvider provider = new TopologyProvider();
        Topology topology = provider.getTopology();

        // Step 3: Initialize and start the streaming application
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        // Step 4: Produce some input data to the input topic.
        produceInputData();

        // Step 5: Verify the application's output data.
        verifyOutputData(streams);

        streams.close();
    }

    private Properties getStreamsConfiguration() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, 
            "wordcount-lambda-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, 
            CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, 
            Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, 
            Serdes.String().getClass().getName());
        // The commit interval for flushing records to state stores and downstream must be lower
        // than this integration test's timeout (30 secs) to ensure we observe the expected 
        // processing results.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Use a temporary directory for storing state, which will be automatically 
        // removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, 
            TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }

    private void produceInputData() throws Exception {
        List<String> inputValues = Arrays.asList(
            "Hello Kafka Streams",
            "All streams lead to Kafka",
            "Join Kafka Summit"
        );
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(
            inputTopic, inputValues, producerConfig, Time.SYSTEM);
    }

    private void verifyOutputData(KafkaStreams streams) throws Exception {
        List<KeyValue<String, Long>> expectedWordCounts = Arrays.asList(
            new KeyValue<>("hello", 1L),
            new KeyValue<>("all", 1L),
            new KeyValue<>("streams", 2L),
            new KeyValue<>("lead", 1L),
            new KeyValue<>("to", 1L),
            new KeyValue<>("join", 1L),
            new KeyValue<>("kafka", 3L),
            new KeyValue<>("summit", 1L)
        );
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, 
            "wordcount-lambda-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            LongDeserializer.class);
        
        // Assert that the actual word counts match the expected word counts
        List<KeyValue<String, Long>> actualWordCounts = 
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig, outputTopic, expectedWordCounts.size());
        assertThat(actualWordCounts, containsInAnyOrder(expectedWordCounts.toArray()));
    }
}
