// event schema
package com.featurestore.flink;

public class Event {
    public String entityId;
    public double amount;
    public int eventType;
    public long timestamp;

    public Event() {}
    
    public Event(String entityId, double amount, int eventType, long timestamp) {
        this.entityId = entityId;
        this.amount = amount;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "user=" + entityId + " totalAmount=" + amount + " eventType=" + eventType + " timestamp=" + timestamp;
    }
}