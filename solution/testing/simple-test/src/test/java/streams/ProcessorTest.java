package streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.Topology;

import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import org.junit.*;

public class ProcessorTest {
    private TopologyTestDriver testDriver;
    private KeyValueStore<String, Long> store;

    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Long> longSerde = new Serdes.LongSerde();
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @Before
    public void setup() {
        TopologyProvider provider = new TopologyProvider();
        Topology topology = provider.getTopology();

        ConfigProvider configProvider = new ConfigProvider();
        Properties config = configProvider.getConfig("dummy-bootstrap-server:9092");

        testDriver = new TopologyTestDriver(topology, config);

        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

        // pre-populate store
        store = testDriver.getKeyValueStore("aggStore");
        store.put("a", 21L);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldFlushStoreForFirstInput() {
        // TODO: add test code here
        inputTopic.pipeInput("a", 1L);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
       }

    @Test
    public void shouldNotUpdateStoreForSmallerValue() {
        // TODO: add test code here
        inputTopic.pipeInput("a", 1L);
        assertThat(store.get("a"), equalTo(21L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldUpdateStoreForLargerValue() {
        // TODO: add test code here
        inputTopic.pipeInput("a", 42L);
        assertThat(store.get("a"), equalTo(42L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 42L)));
        assertThat(outputTopic.isEmpty(), is(true));
        }

    @Test
    public void shouldUpdateStoreForNewKey() {
        // TODO: add test code here
        inputTopic.pipeInput("b", 21L);
        assertThat(store.get("b"), equalTo(21L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("b", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldPunctuateIfEventTimeAdvances() {
        // TODO: add test code here
        final Instant recordTime = Instant.now();
        inputTopic.pipeInput("a", 1L, recordTime);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));

        inputTopic.pipeInput("a", 1L, recordTime);
        assertThat(outputTopic.isEmpty(), is(true));

        inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldPunctuateIfWallClockTimeAdvances() {
        // TODO: add test code here
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }
}
