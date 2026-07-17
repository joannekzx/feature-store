package com.featurestore.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

// the streaming pipeline: read events from Kafka, aggregate per user in 30s windows,
// then fan out to Redis (online serving) and S3 Parquet (offline history)
public class FeatureJob {
    public static void main(String[] args) throws Exception {
        // Hadoop is on the classpath (for Parquet), which makes Flink's security module try to
        // fetch Hadoop delegation tokens at startup -> UserGroupInformation.getSubject, which is
        // removed in JDK 24+. We don't use delegation tokens, so turn the framework off.
        Configuration conf = new Configuration();
        conf.setString("security.delegation.tokens.enabled", "false");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // single parallelism keeps the demo simple: one feature buffer, clean Parquet file set
        env.setParallelism(1);

        // connects to Kafka and reads from the events topic
        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("events")
                .setGroupId("flink-feature-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new EventDeserializer())
                .build();

        DataStream<Event> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // keys the stream by entityId so each user's events are grouped together
        DataStream<Event> aggregated = stream
            .keyBy(event -> event.entityId)
            // opens a 30-second tumbling window - every 30 seconds it closes the window and computes the result
            .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
            .reduce((event1, event2) -> new Event(
                    event1.entityId,
                    // sums the amount field per user within that window
                    event1.amount + event2.amount,
                    Math.max(event1.eventType, event2.eventType),
                    Math.max(event1.timestamp, event2.timestamp)
            ));

        // print for visibility
        aggregated.print();

        // ONLINE store: overwrite latest feature value per entity (for gRPC serving)
        aggregated.addSink(new RedisSink("localhost", 6379));

        // OFFLINE store: append full feature history to Parquet on S3, partitioned by date
        String bucket = System.getenv().getOrDefault("FEATURE_BUCKET", "feature-store-joanne-demo");
        String region = System.getenv().getOrDefault("AWS_REGION", "ap-southeast-1");
        aggregated.addSink(new S3ParquetSink(bucket, region, "features", 10));

        env.execute("Feature Job");
    }    
}

