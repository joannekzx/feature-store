package com.featurestore.grpc;

import com.featurestore.grpc.FeatureStoreProto.GetFeaturesBatchRequest;
import com.featurestore.grpc.FeatureStoreProto.GetFeaturesBatchResponse;
import com.featurestore.grpc.FeatureStoreProto.GetFeaturesRequest;
import com.featurestore.grpc.FeatureStoreProto.GetFeaturesResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FeatureServer {
    private static final int DEFAULT_PORT = 50051;
    private static final String DEFAULT_REDIS_HOST = "localhost";
    private static final int DEFAULT_REDIS_PORT = 6379;

    private final int port;
    private final JedisPool jedisPool;
    private Server server;

    public FeatureServer(int port, String redisHost, int redisPort) {
        this.port = port;
        this.jedisPool = new JedisPool(redisHost, redisPort);
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new FeatureStoreImpl(jedisPool))
                .build()
                .start();
        System.out.println("Server started, listening on " + port);
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = DEFAULT_PORT;
        String redisHost = DEFAULT_REDIS_HOST;
        int redisPort = DEFAULT_REDIS_PORT;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "--redis-host":
                    redisHost = args[++i];
                    break;
                case "--redis-port":
                    redisPort = Integer.parseInt(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }

        FeatureServer server = new FeatureServer(port, redisHost, redisPort);
        server.start();
        server.blockUntilShutdown();
    }

    static class FeatureStoreImpl extends FeatureStoreGrpc.FeatureStoreImplBase {

        private final JedisPool jedisPool;

        public FeatureStoreImpl(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
        }

        @Override
        public void getFeatures(GetFeaturesRequest request, StreamObserver<GetFeaturesResponse> responseObserver) {
            String entityId = request.getEntityId();

            try (Jedis jedis = jedisPool.getResource()) {
                Map<String, String> fields = jedis.hgetAll(keyFor(entityId));
                responseObserver.onNext(toResponse(entityId, fields));
                responseObserver.onCompleted();
            } catch (RuntimeException e) {
                responseObserver.onError(
                        Status.INTERNAL
                                .withDescription("Failed to load features for entity " + entityId)
                                .withCause(e)
                                .asRuntimeException());
            }
        }

        /**
         * Fetch many entities in one RPC. Two Redis access strategies, selected by the
         * request flag, so the load test can measure the same work done two ways:
         *
         *   pipelined = false -> NAIVE "N+1": one hgetAll round-trip per entity. Each call
         *                        pays a full network + Redis-processing latency in series,
         *                        so total time grows linearly with the number of entities.
         *   pipelined = true  -> FIXED: queue all hgetAll commands, flush them in a single
         *                        round-trip, then read the batched replies. One RTT instead of N.
         */
        @Override
        public void getFeaturesBatch(GetFeaturesBatchRequest request,
                                     StreamObserver<GetFeaturesBatchResponse> responseObserver) {
            List<String> entityIds = request.getEntityIdsList();

            try (Jedis jedis = jedisPool.getResource()) {
                GetFeaturesBatchResponse.Builder batch = GetFeaturesBatchResponse.newBuilder();

                if (request.getPipelined()) {
                    // FIXED: one round-trip for all K lookups.
                    Pipeline pipeline = jedis.pipelined();
                    List<Response<Map<String, String>>> pending = new ArrayList<>(entityIds.size());
                    for (String entityId : entityIds) {
                        pending.add(pipeline.hgetAll(keyFor(entityId)));
                    }
                    pipeline.sync();
                    for (int i = 0; i < entityIds.size(); i++) {
                        batch.addFeatures(toResponse(entityIds.get(i), pending.get(i).get()));
                    }
                } else {
                    // NAIVE: K separate round-trips, in series.
                    for (String entityId : entityIds) {
                        Map<String, String> fields = jedis.hgetAll(keyFor(entityId));
                        batch.addFeatures(toResponse(entityId, fields));
                    }
                }

                responseObserver.onNext(batch.build());
                responseObserver.onCompleted();
            } catch (RuntimeException e) {
                responseObserver.onError(
                        Status.INTERNAL
                                .withDescription("Failed to load batch of " + entityIds.size() + " entities")
                                .withCause(e)
                                .asRuntimeException());
            }
        }

        private static String keyFor(String entityId) {
            return "features:" + entityId;
        }

        private static GetFeaturesResponse toResponse(String entityId, Map<String, String> fields) {
            return GetFeaturesResponse.newBuilder()
                    .setEntityId(entityId)
                    .setTotalAmount(Double.parseDouble(fields.getOrDefault("totalAmount", "0")))
                    .setEventType(Integer.parseInt(fields.getOrDefault("eventType", "0")))
                    .setTimestamp(Long.parseLong(fields.getOrDefault("timestamp", "0")))
                    .build();
        }
    }
}
