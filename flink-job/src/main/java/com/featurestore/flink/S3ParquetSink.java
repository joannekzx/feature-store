package com.featurestore.flink;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OFFLINE store sink (Step 6). Appends every windowed feature record to Parquet files on S3,
 * partitioned by date=YYYY-MM-DD derived from the feature's as-of timestamp.
 *
 * Unlike {@link RedisSink} (which overwrites the latest value per entity for online serving),
 * this keeps the full HISTORY of feature records so the point-in-time join in Step 7 has
 * something to join a label back to.
 *
 * Records are buffered and flushed either when the batch fills up or on a periodic timer, so
 * files still land during a slow demo stream. Each flush writes a local temp Parquet file and
 * uploads it with the AWS SDK -- no Flink S3 filesystem plugin or checkpointing required.
 */
public class S3ParquetSink extends RichSinkFunction<Event> {

    static final Schema SCHEMA = SchemaBuilder.record("FeatureRecord")
            .namespace("com.featurestore.flink")
            .fields()
            .requiredString("entity_id")
            .requiredDouble("total_amount")
            .requiredInt("event_type")
            .requiredLong("feature_timestamp")
            .endRecord();

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final String bucket;
    private final String region;
    private final String prefix;
    private final int batchSize;

    private transient S3Client s3;
    private transient List<Event> buffer;
    private transient ScheduledExecutorService flusher;
    private transient AtomicLong fileCounter;
    private transient int subtask;

    public S3ParquetSink(String bucket, String region, String prefix, int batchSize) {
        this.bucket = bucket;
        this.region = region;
        this.prefix = prefix;
        this.batchSize = batchSize;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        s3 = S3Client.builder().region(Region.of(region)).build();
        buffer = new ArrayList<>();
        fileCounter = new AtomicLong();
        subtask = getRuntimeContext().getIndexOfThisSubtask();
        // flush partial batches on a timer so records land even when the stream is slow
        flusher = Executors.newSingleThreadScheduledExecutor();
        flusher.scheduleWithFixedDelay(this::flushQuietly, 20, 20, TimeUnit.SECONDS);
    }

    // buffer each record, flushing once we've collected a full batch
    @Override
    public void invoke(Event event, Context context) {
        synchronized (this) {
            buffer.add(event);
            if (buffer.size() >= batchSize) {
                flush();
            }
        }
    }

    // timer-driven flush that swallows errors so one bad flush doesn't kill the scheduler
    private void flushQuietly() {
        try {
            synchronized (this) {
                flush();
            }
        } catch (Exception e) {
            System.err.println("S3 flush failed: " + e);
        }
    }

    /** Caller must hold this sink's monitor. */
    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        Map<String, List<Event>> byDate = new LinkedHashMap<>();
        for (Event e : buffer) {
            String date = Instant.ofEpochMilli(e.timestamp).atZone(ZoneOffset.UTC).toLocalDate().format(DATE_FMT);
            byDate.computeIfAbsent(date, d -> new ArrayList<>()).add(e);
        }
        for (Map.Entry<String, List<Event>> entry : byDate.entrySet()) {
            try {
                writePartition(entry.getKey(), entry.getValue());
            } catch (Exception ex) {
                throw new RuntimeException("Failed writing partition date=" + entry.getKey(), ex);
            }
        }
        buffer.clear();
    }

    // write one date's rows to a local parquet file, then upload it under date=YYYY-MM-DD/ on S3
    private void writePartition(String date, List<Event> events) throws Exception {
        File tmp = File.createTempFile("features-", ".parquet");
        tmp.delete(); // the Parquet writer needs to create the file itself
        // LocalOutputFile writes via java.nio, avoiding Hadoop's FileSystem/UserGroupInformation
        // (which calls the JDK's removed Subject.getSubject and breaks on JDK 24+).
        OutputFile out = new LocalOutputFile(tmp.toPath());
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(out)
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (Event e : events) {
                GenericRecord rec = new GenericData.Record(SCHEMA);
                rec.put("entity_id", e.entityId);
                rec.put("total_amount", e.amount);
                rec.put("event_type", e.eventType);
                rec.put("feature_timestamp", e.timestamp);
                writer.write(rec);
            }
        }
        String key = prefix + "/date=" + date + "/part-" + subtask + "-" + fileCounter.getAndIncrement() + ".parquet";
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(tmp));
        System.out.println("Wrote " + events.size() + " rows to s3://" + bucket + "/" + key);
        tmp.delete();
    }

    // stop the timer and flush whatever's left so buffered records aren't lost on shutdown
    @Override
    public void close() {
        if (flusher != null) {
            flusher.shutdownNow();
        }
        synchronized (this) {
            try {
                flush();
            } catch (Exception e) {
                System.err.println("Final S3 flush failed: " + e.getMessage());
            }
        }
        if (s3 != null) {
            s3.close();
        }
    }
}
