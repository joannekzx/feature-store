package com.featurestore.flink;

import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

// converts raw JSON bytes from Kafka into an Event object that Flink can work with
public class EventDeserializer implements DeserializationSchema<Event> {
    private static final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, Event.class);
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}