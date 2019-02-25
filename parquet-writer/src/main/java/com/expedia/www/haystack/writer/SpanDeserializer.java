package com.expedia.www.haystack.writer;

import com.expedia.open.tracing.Span;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class SpanDeserializer implements Deserializer<Span>  {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        /* do nothing */
    }

    @Override
    public Span deserialize(String s, byte[] bytes) {
        try {
            return Span.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            log.error("Fail to deserialize the span bytes", e);
            return null;
        }
    }

    @Override
    public void close() {
        /* do nothing */
    }
}
