package streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class WordCountTransformer implements Transformer<String, String, KeyValue<String, Long>> {

  private KeyValueStore<String, Long> kvStore;

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    context.schedule(
        Duration.ofMillis(5_000),
        PunctuationType.STREAM_TIME,
        timestamp -> {
          KeyValueIterator<String, Long> iter = kvStore.all();
          System.out.println("------ " + context.taskId() + " - " + timestamp + " -----" + " ");
          while (iter.hasNext()) {
            KeyValue<String, Long> entry = iter.next();
            System.out.println("[" + entry.key + ", " + entry.value + "]");
            context.forward(entry.key, entry.value);
          }
        });
    this.kvStore = (KeyValueStore<String, Long>) context.getStateStore("Counts");
  }

  @Override
  public KeyValue<String, Long> transform(String word, String dummy) {
    // TODO: Get the correct entry from the keystore and update it or create an intial entry
    return null;
  }

  @Override
  public void close() {}
}
