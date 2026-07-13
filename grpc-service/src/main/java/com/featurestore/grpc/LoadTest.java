package com.featurestore.grpc;

import com.featurestore.grpc.FeatureStoreProto.GetFeaturesBatchRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Step 8 -- serving-latency benchmark for the N+1-lookups bottleneck.
 *
 * Fires the same GetFeaturesBatch RPC many times in two modes and reports p50/p99:
 *   NAIVE     -> server does one Redis round-trip per entity (N+1)
 *   PIPELINED -> server does a single pipelined round-trip for the whole batch
 *
 * The number that matters is the p99 gap between the two modes: it isolates the
 * cost of the extra round-trips, since everything else (payload, gRPC, parsing) is
 * identical. Latency is measured client-side with System.nanoTime() around each call.
 *
 * Usage:
 *   java ... LoadTest [host] [port] [batchSize] [iterations]
 * Defaults: 127.0.0.1 50051 50 2000
 */
public class LoadTest {

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 50051;
        int batchSize = args.length > 2 ? Integer.parseInt(args[2]) : 50;
        int iterations = args.length > 3 ? Integer.parseInt(args[3]) : 2000;

        // The pipeline is seeded with 10 entities (user-0..user-9); cycle through them
        // to build a batch of the requested size.
        List<String> entityIds = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            entityIds.add("user-" + (i % 10));
        }

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        try {
            FeatureStoreGrpc.FeatureStoreBlockingStub stub = FeatureStoreGrpc.newBlockingStub(channel);

            GetFeaturesBatchRequest naiveReq = GetFeaturesBatchRequest.newBuilder()
                    .addAllEntityIds(entityIds).setPipelined(false).build();
            GetFeaturesBatchRequest pipelinedReq = GetFeaturesBatchRequest.newBuilder()
                    .addAllEntityIds(entityIds).setPipelined(true).build();

            System.out.printf("Benchmark: batchSize=%d entities, iterations=%d per mode%n%n",
                    batchSize, iterations);

            // Warm up the JVM / JIT / connections so the measured runs are steady-state.
            warmUp(stub, naiveReq, pipelinedReq);

            long[] naive = measure(stub, naiveReq, iterations);
            long[] pipelined = measure(stub, pipelinedReq, iterations);

            report("NAIVE  (N+1: one round-trip per entity)", naive);
            report("PIPELINED (single batched round-trip)  ", pipelined);

            double p50Before = percentile(naive, 50), p50After = percentile(pipelined, 50);
            double p99Before = percentile(naive, 99), p99After = percentile(pipelined, 99);

            System.out.println();
            System.out.println("| mode                     | p50 (ms) | p99 (ms) |");
            System.out.println("|--------------------------|----------|----------|");
            System.out.printf("| Before (N+1 lookups)     | %8.3f | %8.3f |%n", p50Before, p99Before);
            System.out.printf("| After  (pipelined batch) | %8.3f | %8.3f |%n", p50After, p99After);
            System.out.println();
            System.out.printf("Speedup: p50 %.1fx, p99 %.1fx%n",
                    p50Before / p50After, p99Before / p99After);
        } finally {
            channel.shutdownNow();
        }
    }

    private static void warmUp(FeatureStoreGrpc.FeatureStoreBlockingStub stub,
                               GetFeaturesBatchRequest naiveReq,
                               GetFeaturesBatchRequest pipelinedReq) {
        for (int i = 0; i < 200; i++) {
            stub.getFeaturesBatch(naiveReq);
            stub.getFeaturesBatch(pipelinedReq);
        }
    }

    /** Run {@code iterations} calls, returning per-call latencies in nanoseconds. */
    private static long[] measure(FeatureStoreGrpc.FeatureStoreBlockingStub stub,
                                  GetFeaturesBatchRequest request, int iterations) {
        long[] latencies = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            long start = System.nanoTime();
            stub.getFeaturesBatch(request);
            latencies[i] = System.nanoTime() - start;
        }
        return latencies;
    }

    private static void report(String label, long[] latenciesNs) {
        System.out.printf("%s -> p50=%.3fms  p99=%.3fms  max=%.3fms%n",
                label, percentile(latenciesNs, 50), percentile(latenciesNs, 99),
                percentile(latenciesNs, 100));
    }

    /** Percentile in milliseconds. p=100 returns the max. Sorts a copy first. */
    private static double percentile(long[] latenciesNs, int p) {
        long[] sorted = latenciesNs.clone();
        Arrays.sort(sorted);
        int idx = p >= 100 ? sorted.length - 1 : (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        idx = Math.max(0, Math.min(idx, sorted.length - 1));
        return sorted[idx] / 1_000_000.0;
    }
}
