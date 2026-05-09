package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.FileDao;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.grpc.InternalKvServiceGrpc;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.grpc.InternalRequest;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.grpc.InternalResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ShardedFileKVService implements KVService {

    private static final String LOCALHOST = "http://localhost:";
    private static final Logger log = LoggerFactory.getLogger(ShardedFileKVService.class);
    private final HttpServer server;
    private final Dao<byte[]> storage;
    private final String selfUrl;
    private final HashingStrategy hashingStrategy;
    private io.grpc.Server grpcServer;
    private int grpcPort;
    private final Map<String, GrpcClient> grpcClients = new ConcurrentHashMap<>();

    public ShardedFileKVService(int port, HashingStrategy hashingStrategy) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.storage = new FileDao(Path.of("ekaterina-gavrilova-storage-" + port));
        this.selfUrl = LOCALHOST + port;
        this.hashingStrategy = hashingStrategy;

        initServer();
    }

    public void setNodes(List<String> nodes) {
        this.hashingStrategy.setEndpoints(nodes);
        this.grpcPort = extractGrpcPortFromNodes(nodes, selfUrl);
    }

    @Override
    public void start() {
        server.start();
        startGrpcServer();
        log.info("ShardedFileKVService started at {}", selfUrl);
    }

    @Override
    public void stop() {
        server.stop(0);
        if (grpcServer != null) {
            grpcServer.shutdownNow();
        }
        for (GrpcClient client : grpcClients.values()) {
            client.channel.shutdownNow();
        }
        grpcClients.clear();
        log.info("ShardedFileKVService stopped at {}", selfUrl);
    }

    private void initServer() {
        createContexts();
        server.setExecutor(Executors.newFixedThreadPool(8));
        log.info("Server initialized at {}", selfUrl);
    }

    private void createContexts() {
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
    }

    private String resolveNode(String key) {
        return hashingStrategy.getNode(key);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!Objects.equals(requestMethod, "GET")) {
            sendResponse(exchange, 405, "Method Not Allowed".getBytes());
            return;
        }
        sendResponse(exchange, 200, "OK".getBytes());
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String id = extractId(exchange);
            if (id == null) {
                sendResponse(exchange, 400, "Missing id parameter".getBytes());
                return;
            }

            String targetNode = resolveNode(id);
            String targetHttpUrl = targetNode.split("\\?")[0];

            if (!selfUrl.equals(targetHttpUrl)) {
                proxyRequest(exchange, targetNode, id);
                return;
            }
            handleLocal(exchange, id);

        } catch (Exception e) {
            log.error("Error handling /v0/entity", e);
            sendResponse(exchange, 500, "Internal Server Error".getBytes());
        }
    }

    private void handleLocal(HttpExchange exchange, String id) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> sendResponse(exchange, 405, "Method Not Allowed".getBytes());
        }
    }

    private void proxyRequest(HttpExchange exchange, String target, String id) throws IOException {
        try {
            String host = "localhost";
            int grpcPort = extractGrpcPort(target);

            byte[] body = exchange.getRequestBody().readAllBytes();

            InternalRequest request = InternalRequest.newBuilder()
                    .setMethod(exchange.getRequestMethod())
                    .setKey(id)
                    .setBody(com.google.protobuf.ByteString.copyFrom(body))
                    .build();

            GrpcClient client = getClient(host, grpcPort);

            InternalResponse response = client.stub
                    .withDeadlineAfter(200, TimeUnit.MILLISECONDS)
                    .processRequest(request);

            sendResponse(exchange,
                    response.getStatusCode(),
                    response.getBody().toByteArray());

        } catch (Exception e) {
            log.error("gRPC proxy error", e);
            sendResponse(exchange, 500, "gRPC error".getBytes());
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] value = storage.get(id);
            sendResponse(exchange, 200, value);
        } catch (Exception e) {
            sendResponse(exchange, 404, "Not Found".getBytes());
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try (InputStream inputStream = exchange.getRequestBody()) {
            byte[] data = inputStream.readAllBytes();
            storage.upsert(id, data);
            sendResponse(exchange, 201, "Created".getBytes());
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        storage.delete(id);
        sendResponse(exchange, 202, "Accepted".getBytes());
    }

    private String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null || !query.startsWith("id=")) {
            return null;
        }

        String id = URLDecoder.decode(query.substring(3), StandardCharsets.UTF_8);
        return id.isBlank() ? null : id;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, byte[] bytes) throws IOException {
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private GrpcClient getClient(String host, int port) {
        String key = host + ":" + port;

        return grpcClients.computeIfAbsent(key, k -> {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext()
                    .build();

            InternalKvServiceGrpc.InternalKvServiceBlockingStub stub =
                    InternalKvServiceGrpc.newBlockingStub(channel);

            return new GrpcClient(channel, stub);
        });
    }

    private int extractGrpcPort(String endpoint) {
        String[] parts = endpoint.split("\\?grpc=");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Bad endpoint: " + endpoint);
        }
        return Integer.parseInt(parts[1]);
    }

    private int extractGrpcPortFromNodes(List<String> nodes, String selfUrl) {
        String httpPort = selfUrl.substring(LOCALHOST.length()).split("\\?")[0];

        return nodes.stream()
                .filter(n -> n.startsWith(LOCALHOST + httpPort))
                .map(n -> Integer.parseInt(n.split("\\?grpc=")[1]))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("gRPC port not found"));
    }

    public byte[] localGet(String id) throws IOException {
        return storage.get(id);
    }

    public void localPut(String id, byte[] data) throws IOException {
        storage.upsert(id, data);
    }

    public void localDelete(String id) throws IOException {
        storage.delete(id);
    }

    private void startGrpcServer() {
        try {
            grpcServer = io.grpc.ServerBuilder
                    .forPort(grpcPort)
                    .addService(new GrpcInternalService(this))
                    .build()
                    .start();

            log.info("gRPC server started on port {}", grpcPort);

        } catch (IOException e) {
            throw new RuntimeException("Failed to start gRPC server", e);
        }
    }

    private static final class GrpcClient {
        final ManagedChannel channel;
        final InternalKvServiceGrpc.InternalKvServiceBlockingStub stub;

        public GrpcClient(ManagedChannel channel,
                          InternalKvServiceGrpc.InternalKvServiceBlockingStub stub) {
            this.channel = channel;
            this.stub = stub;
        }
    }

}
