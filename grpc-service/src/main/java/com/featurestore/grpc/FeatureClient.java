package com.featurestore.grpc;

import com.featurestore.grpc.FeatureStoreProto.GetFeaturesRequest;
import com.featurestore.grpc.FeatureStoreProto.GetFeaturesResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class FeatureClient {

    public static void main(String[] args) throws InterruptedException {
        String host = "127.0.0.1";
        int port = 50051;

        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        try {
            FeatureStoreGrpc.FeatureStoreBlockingStub stub = FeatureStoreGrpc.newBlockingStub(channel);
            String[] users = {"user-0", "user-1", "user-2", "user-3"};

            for (String userId : users) {
                GetFeaturesRequest request = GetFeaturesRequest.newBuilder()
                        .setEntityId(userId)
                        .build();

                GetFeaturesResponse response = stub.getFeatures(request);
                System.out.println("entity=" + response.getEntityId()
                        + " totalAmount=" + response.getTotalAmount()
                        + " eventType=" + response.getEventType()
                        + " timestamp=" + response.getTimestamp());
            }
        } finally {
            channel.shutdownNow();
        }
    }
}