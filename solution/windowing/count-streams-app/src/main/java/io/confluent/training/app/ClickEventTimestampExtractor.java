package io.confluent.training.app;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import io.confluent.training.proto.ClicksProtos;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;


public class ClickEventTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        try {
            return ClicksProtos.Clicks.parseFrom(((DynamicMessage)record.value()).toByteString()).getTimestamp();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return previousTimestamp;
    }

}


