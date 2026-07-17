package com.featurestore.flink;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

// converts raw JSON bytes from Kafka into an Event object that Flink can work with.
// before mapping, every payload is validated against FeatureSchema (this is the write-path)
// gate that stops a broken upstream change from reaching Redis or Parquet.
public class EventDeserializer implements DeserializationSchema<Event> {
    // ignore unknown props so the schemaVersion field (validated below, but unused by Event) doesn't blow up the mapping
    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // running tally of schema-rejected events, so the failure is demonstrable in the log rather than theoretical
    private static final AtomicLong rejected = new AtomicLong();

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        JsonNode node = mapper.readTree(bytes);
        String reason = FeatureSchema.validate(node);
        if (reason != null) {
            long n = rejected.incrementAndGet();
            // returning null tells Flink to drop the record so it never becomes a feature
            System.err.println("SCHEMA REJECT v" + FeatureSchema.CURRENT_VERSION
                    + " (rejected so far: " + n + "): " + reason + " -> " + node);
            return null;
        }
        return mapper.treeToValue(node, Event.class);
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
