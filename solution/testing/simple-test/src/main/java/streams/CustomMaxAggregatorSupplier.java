package streams;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;

public class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long> {
    
    public class CustomMaxAggregator implements Processor<String, Long> {
        ProcessorContext context;
        private KeyValueStore<String, Long> store;
    
        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            context.schedule(Duration.ofMillis(60000), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    flushStore();
                }
            });
            context.schedule(Duration.ofMillis(10000), PunctuationType.STREAM_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    flushStore();
                }
            });
            store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
        }
    
        @Override
        public void process(String key, Long value) {
            Long oldValue = store.get(key);
            if (oldValue == null || value > oldValue) {
                store.put(key, value);
            }
        }
    
        private void flushStore() {
            KeyValueIterator<String, Long> it = store.all();
            while (it.hasNext()) {
                KeyValue<String, Long> next = it.next();
                context.forward(next.key, next.value);
            }
        }
    
        @Override
        public void close() {}
    }    

    @Override
    public Processor<String, Long> get() {
        return new CustomMaxAggregator();
    }
}
