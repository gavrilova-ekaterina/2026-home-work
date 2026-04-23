package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import static company.vk.edu.distrib.compute.gavrilova_ekaterina.RequestParserHelper.extractAck;
import static company.vk.edu.distrib.compute.gavrilova_ekaterina.RequestParserHelper.extractId;

public class ReplicatedKVServiceImpl implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedKVServiceImpl.class);
    private final int replicationFactor;
    private final int localPort;
    private final HttpServer server;
    private final ReplicaSynchronizer replicaSynchronizer;
    private final List<ReplicaNode> replicas = new ArrayList<>();
    private final Set<String> keys = ConcurrentHashMap.newKeySet();

    public ReplicatedKVServiceImpl(int port) throws IOException {
        this.localPort = port;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.replicationFactor = ReplicationConfigUtils.getReplicationFactor();

        initReplicas();
        initServer();

        this.replicaSynchronizer = new ReplicaSynchronizer(replicas);
    }

    private void initReplicas() throws IOException {
        for (int i = 0; i < replicationFactor; i++) {
            replicas.add(new ReplicaNode(i));
        }
    }

    @Override
    public void start() {
        server.start();
        log.info("ReplicationKVService started");
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("ReplicationKVService stopped");
    }

    @Override
    public int port() {
        return localPort;
    }

    @Override
    public int numberOfReplicas() {
        return replicas.size();
    }

    @Override
    public void disableReplica(int nodeId) {
        replicas.get(nodeId).alive = false;
        log.warn("Replica disabled nodeId={}", nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        ReplicaNode node = replicas.get(nodeId);
        node.alive = true;
        log.warn("Replica enabled nodeId={} -> starting sync", nodeId);
        try {
            replicaSynchronizer.syncReplica(node, keys);
            log.info("Replica sync completed nodeId={}", nodeId);
        } catch (IOException e) {
            log.error("Replica sync failed nodeId={}", nodeId, e);
        }
    }

    private void initServer() {
        if (server == null) {
            log.error("Server is null");
        } else {
            createContexts();
            log.info("Server is initialized");
        }
    }

    private void createContexts() {
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
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
        try {
            String id = extractId(exchange);
            if (id == null) {
                sendResponse(exchange, 400, "Missing id".getBytes());
                return;
            }

            int ack = extractAck(exchange);
            if (ack > replicas.size() || ack <= 0) {
                sendResponse(exchange, 400, new byte[0]);
            }

            switch (exchange.getRequestMethod()) {
                case "GET" -> handleGet(exchange, id, ack);
                case "PUT" -> handlePut(exchange, id, ack);
                case "DELETE" -> handleDelete(exchange, id, ack);
                default -> sendResponse(exchange, 405, "Method Not Allowed".getBytes());
            }

        } catch (Exception e) {
            log.error("Error handling request", e);
            sendResponse(exchange, 500, "Internal Error".getBytes());
        }
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        int responded = 0;
        byte[] value = null;
        log.info("GET start id={}, ack={}", id, ack);

        for (ReplicaNode node : getReplicasForKey(id)) {
            if (!node.alive) {
                continue;
            }
            try {
                byte[] v = node.dao.get(id);
                if (v != null) {
                    value = v;
                }
                responded++;
            } catch (NoSuchElementException e) {
                responded++;
            }
        }
        if (responded < ack) {
            log.warn("GET quorum failed id={}, responded={}, ack={}", id, responded, ack);
            sendResponse(exchange, 500, new byte[0]);
            return;
        }
        if (value == null) {
            log.info("GET result not found for id={}, responded={}, ack={}", id, responded, ack);
            sendResponse(exchange, 404, new byte[0]);
        } else {
            log.info("GET result success id={}, responded={}, ack={}", id, responded, ack);
            sendResponse(exchange, 200, value);
        }
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        int responded = 0;
        log.info("PUT start id={}, size={}, ack={}", id, body.length, ack);
        for (ReplicaNode node : getReplicasForKey(id)) {
            if (!node.alive) {
                continue;
            }
            try {
                node.dao.upsert(id, body);
                responded++;
            } catch (Exception e) {
                log.warn("PUT replica failed id={}", id);
            }
        }
        keys.add(id);
        if (responded < ack) {
            log.warn("PUT quorum failed id={}, responded={}, ack={}", id, responded, ack);
            sendResponse(exchange, 500, new byte[0]);
            return;
        }
        log.info("PUT result success id={}, responded={}, ack={}", id, responded, ack);
        sendResponse(exchange, 201, new byte[0]);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        int responded = 0;
        log.info("DELETE start id={}, ack={}", id, ack);
        for (ReplicaNode node : getReplicasForKey(id)) {
            if (!node.alive) {
                continue;
            }
            try {
                node.dao.delete(id);
                responded++;
            } catch (Exception e) {
                log.warn("DELETE replica failed id={}", id);
            }
        }
        if (responded < ack) {
            log.warn("DELETE quorum failed id={}, success={}, ack={}", id, responded, ack);
            sendResponse(exchange, 500, new byte[0]);
            return;
        }
        log.info("DELETE success id={}, success={}, ack={}", id, responded, ack);
        sendResponse(exchange, 202, new byte[0]);
    }

    private List<ReplicaNode> getReplicasForKey(String key) {
        int start = Math.abs(key.hashCode()) % replicas.size();
        List<ReplicaNode> result = new ArrayList<>();

        for (int i = 0; i < replicas.size(); i++) {
            result.add(replicas.get((start + i) % replicas.size()));
        }
        return result;
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

}
