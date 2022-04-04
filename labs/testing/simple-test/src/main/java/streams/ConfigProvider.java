package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import java.util.Properties;

public class ConfigProvider {
    public Properties getConfig(String bootstrapServers) {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        return config;
    }
}
