package company.vk.edu.distrib.compute.gavrilova_ekaterina.kafka;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class AuditableKVServiceImpl implements AuditableKVService {

    private static final String AUDIT_TOPIC = "audit";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final byte[] EMPTY_BODY = new byte[0];
    private static final int OK = 200;
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int BAD_REQUEST = 400;
    private static final int NOT_FOUND = 404;
    private static final int METHOD_NOT_ALLOWED = 405;

    private final int port;
    private final Map<String, byte[]> data = new ConcurrentHashMap<>();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private boolean async = true;
    private Producer<String, String> producer;
    private HttpServer server;

    public AuditableKVServiceImpl(int port) {
        this.port = port;
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        Producer<String, String> oldProducer = producer;
        producer = new KafkaProducer<>(producerProperties(bootstrapServers));
        if (oldProducer != null) {
            oldProducer.close();
        }
    }

    @Override
    public void setAsync(boolean enabled) {
        async = enabled;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(ENTITY_PATH, this::handleEntity);
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start audit KV service", e);
        }
    }

    @Override
    public void stop() {
        HttpServer currentServer = server;
        if (currentServer != null) {
            currentServer.stop(0);
            server = null;
        }
        Producer<String, String> currentProducer = producer;
        if (currentProducer != null) {
            currentProducer.flush();
            currentProducer.close();
            producer = null;
        }
        termination.complete(null);
    }

    @Override
    public CompletableFuture<Void> awaitTermination() {
        return termination;
    }

    private Properties producerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        long receivedAt = System.currentTimeMillis();
        String id = extractId(exchange.getRequestURI());
        if (id != null) {
            audit(exchange.getRequestMethod(), id, receivedAt);
        }
        if (id == null || id.isEmpty()) {
            send(exchange, BAD_REQUEST, EMPTY_BODY);
            return;
        }
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id);
            case "PUT" -> handlePut(exchange, id);
            case "DELETE" -> handleDelete(exchange, id);
            default -> send(exchange, METHOD_NOT_ALLOWED, EMPTY_BODY);
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = data.get(id);
        if (value == null) {
            send(exchange, NOT_FOUND, EMPTY_BODY);
        } else {
            send(exchange, OK, value);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        data.put(id, exchange.getRequestBody().readAllBytes());
        send(exchange, CREATED, EMPTY_BODY);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        data.remove(id);
        send(exchange, ACCEPTED, EMPTY_BODY);
    }

    private void audit(String method, String id, long timestamp) {
        Producer<String, String> currentProducer = producer;
        if (currentProducer == null) {
            return;
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(
                AUDIT_TOPIC,
                id,
                AuditEventCodec.serialize(new AuditEvent(method, id, timestamp))
        );
        if (async) {
            currentProducer.send(record);
        } else {
            sendSynchronously(currentProducer, record);
        }
    }

    private void sendSynchronously(Producer<String, String> currentProducer, ProducerRecord<String, String> record) {
        try {
            currentProducer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending audit event", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to send audit event", e);
        }
    }

    private String extractId(URI uri) {
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }
        return Stream.of(query.split("&"))
                .map(parameter -> parameter.split("=", 2))
                .filter(parts -> parts.length == 2 && "id".equals(decode(parts[0])))
                .map(parts -> decode(parts[1]))
                .findFirst()
                .orElse(null);
    }

    private String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private void send(HttpExchange exchange, int statusCode, byte[] response) throws IOException {
        try (exchange) {
            exchange.sendResponseHeaders(statusCode, response.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(response);
            }
        }
    }

}
