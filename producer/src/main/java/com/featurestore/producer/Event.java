// event schema
package com.featurestore.producer;

public class Event {
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