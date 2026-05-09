package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import company.vk.edu.distrib.compute.gavrilova_ekaterina.grpc.InternalKvServiceGrpc;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.grpc.InternalRequest;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.grpc.InternalResponse;

public class GrpcInternalService extends InternalKvServiceGrpc.InternalKvServiceImplBase {

    private final ShardedFileKVService node;

    public GrpcInternalService(ShardedFileKVService node) {
        super();
        this.node = node;
    }

    @Override
    public void processRequest(InternalRequest request,
                               io.grpc.stub.StreamObserver<InternalResponse> responseObserver) {
        try {
            byte[] body = new byte[0];
            int status;

            switch (request.getMethod()) {
                case "GET" -> {
                    try {
                        body = node.localGet(request.getKey());
                        status = 200;
                    } catch (Exception e) {
                        status = 404;
                    }
                }
                case "PUT" -> {
                    node.localPut(request.getKey(), request.getBody().toByteArray());
                    status = 201;
                }
                case "DELETE" -> {
                    node.localDelete(request.getKey());
                    status = 202;
                }
                default -> status = 405;
            }
            InternalResponse response = InternalResponse.newBuilder()
                    .setStatusCode(status)
                    .setBody(com.google.protobuf.ByteString.copyFrom(body))
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

}
