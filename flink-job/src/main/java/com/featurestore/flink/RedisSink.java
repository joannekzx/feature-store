package com.featurestore.flink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

// RichSinkFunction: provides open() and close() lifecyle methods
// open(): called once when sink is initialized, used to create a connection pool to Redis
// invoke(): borrows a connection from the pool, uses it, returns it automatically
// close(): called once when sink is closed, used to close and clean up the connection pool
public class RedisSink extends RichSinkFunction<Event> {

    private final String host;
    private final int port;
    private transient JedisPool pool;

    public RedisSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        pool = new JedisPool(host, port);
    }

    @Override
    public void invoke(Event event, Context context) {
        try (Jedis jedis = pool.getResource()) {
            String key = "features:" + event.entityId;
            // Write all fields in ONE hset (a single round-trip) rather than three
            // separate hset calls -- same online-store lesson as the Step 8 batch benchmark:
            // fewer round-trips to Redis is the win.
            Map<String, String> fields = Map.of(
                    "totalAmount", String.valueOf(event.amount),
                    "eventType", String.valueOf(event.eventType),
                    "timestamp", String.valueOf(event.timestamp));
            jedis.hset(key, fields);
            System.out.println("Written to Redis: " + key + " -> " + event.amount);
        }
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }
}