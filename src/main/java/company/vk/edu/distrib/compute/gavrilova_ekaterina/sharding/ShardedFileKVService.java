package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.gavrilova_ekaterina.FileDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;

public class ShardedFileKVService implements KVService {

    private static final String LOCALHOST = "http://localhost:";
    private static final Logger log = LoggerFactory.getLogger(ShardedFileKVService.class);
    private final HttpServer server;
    private final Dao<byte[]> storage;
    private final String selfUrl;
    private final HashingStrategy hashingStrategy;
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public ShardedFileKVService(int port, HashingStrategy hashingStrategy) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.storage = new FileDao(Path.of("ekaterina-gavrilova-storage-" + port));
        this.selfUrl = LOCALHOST + port;
        this.hashingStrategy = hashingStrategy;

        initServer();
    }

    public void setNodes(List<String> nodes) {
        this.hashingStrategy.setEndpoints(nodes);
    }

    @Override
    public void start() {
        server.start();
        log.info("ShardedFileKVService started at {}", selfUrl);
    }

    @Override
    public void stop() {
        server.stop(0);
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

            if (!selfUrl.equals(targetNode)) {
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
            String url = target + "/v0/entity?id="
                    + URLEncoder.encode(id, StandardCharsets.UTF_8);

            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(1));

            switch (exchange.getRequestMethod()) {
                case "GET" -> builder.GET();

                case "PUT" -> {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                }

                case "DELETE" -> builder.DELETE();

                default -> {
                    sendResponse(exchange, 405, "Method Not Allowed".getBytes());
                    return;
                }
            }

            HttpResponse<byte[]> response = httpClient.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );

            sendResponse(exchange, response.statusCode(), response.body());

        } catch (Exception e) {
            log.error("Unexpected proxy error", e);
            sendResponse(exchange, 500, "Internal server error".getBytes());
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
}
