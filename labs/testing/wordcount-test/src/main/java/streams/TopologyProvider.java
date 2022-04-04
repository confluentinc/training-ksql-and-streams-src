package streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.regex.Pattern;

public class TopologyProvider {
    public Topology getTopology(){
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();
    
        final KStream<String, String> textLines = builder.stream("lines-topic");

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KTable<String, Long> wordCounts = textLines
          .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
          .groupBy((key, word) -> word)
          .count();
    
        wordCounts.toStream().to("word-count-topic", Produced.with(stringSerde, longSerde));

        Topology topology = builder.build();
        return topology;
    }
}
