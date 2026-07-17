// event schema
package com.featurestore.producer;

public class Event {
    // the schema version this event was written against; the Flink job rejects anything it doesn't recognise
    public int schemaVersion = 1;
    public String entityId;
    public double amount;
    public int eventType;
    public long timestamp;

    public Event(String entityId, double amount, int eventType, long timestamp) {
        this.entityId = entityId;
        this.amount = amount;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }
}