package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ShardedKVCluster implements KVCluster {

    private static final String LOCALHOST = "http://localhost:";
    private final List<String> endpoints;
    private final Map<String, ShardedFileKVService> nodesByEndpoints = new ConcurrentHashMap<>();
    private final HashingStrategy hashingStrategy;

    public ShardedKVCluster(List<Integer> ports) {
        this.endpoints = ports.stream()
                .map(this::toEndpoint)
                .toList();
        this.hashingStrategy = resolveHashingStrategy(HashingAlgorithmConfigUtils.getHashingAlgorithm());
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        ShardedFileKVService node = nodesByEndpoints.get(endpoint);

        if (node == null) {
            int port = parsePort(endpoint);

            try {
                node = new ShardedFileKVService(port, hashingStrategy);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to start KV node at endpoint: " + endpoint, e);
            }

            node.setNodes(endpoints);
            nodesByEndpoints.put(endpoint, node);
        }

        node.start();
    }

    @Override
    public void stop() {
        for (KVService node : nodesByEndpoints.values()) {
            node.stop();
        }
        nodesByEndpoints.clear();
    }

    @Override
    public void stop(String endpoint) {
        KVService node = nodesByEndpoints.remove(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(endpoints);
    }

    private String toEndpoint(int port) {
        return LOCALHOST + port;
    }

    private int parsePort(String endpoint) {
        return Integer.parseInt(endpoint.substring(LOCALHOST.length()));
    }

    private HashingStrategy resolveHashingStrategy(HashingAlgorithm hashingAlgorithm) {
        if (hashingAlgorithm == HashingAlgorithm.RENDEZVOUS) {
            return new RendezvousHashingStrategy();
        } else if (hashingAlgorithm == HashingAlgorithm.CONSISTENT) {
            return new ConsistentHashingStrategy();
        }
        return new RendezvousHashingStrategy();
    }

}
