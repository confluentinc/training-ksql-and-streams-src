package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.Topology;

public class TopologyProvider {
    public Topology getTopology() {
        Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("aggStore"),
                Serdes.String(),
                Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
            "aggregator");
        topology.addSink("sinkProcessor", "result-topic", "aggregator");
        return topology;
    }
}
