package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import java.util.List;

public class RendezvousHashingStrategy implements HashingStrategy {

    private static final long FNV_OFFSET_BASIS_64 = 1469598103934665603L;
    private static final long FNV_PRIME_64 = 1099511628211L;
    private static final int SINGLE_ENDPOINT = 1;

    private List<String> endpoints = List.of();

    @Override
    public void setEndpoints(List<String> endpoints) {
        this.endpoints = List.copyOf(endpoints);
    }

    @Override
    public String getNode(String key) {
        if (endpoints.isEmpty()) {
            throw new IllegalStateException("Cluster has no nodes");
        }

        if (endpoints.size() == SINGLE_ENDPOINT) {
            return endpoints.getFirst();
        }

        String bestNode = null;
        long bestScore = 0;

        for (String node : endpoints) {
            long score = hash(key, node);

            if (bestNode == null || Long.compareUnsigned(score, bestScore) > 0) {
                bestScore = score;
                bestNode = node;
            }
        }
        return bestNode;
    }

    private long hash(String key, String node) {
        String s = key + ":" + node;

        long hash = FNV_OFFSET_BASIS_64;

        for (int i = 0; i < s.length(); i++) {
            hash ^= s.charAt(i);
            hash *= FNV_PRIME_64;
        }
        return hash;
    }

}
