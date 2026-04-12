package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

public final class HashingAlgorithmConfigUtils {

    private static final String ENV_KEY = "HASHING_ALGORITHM";
    private static final String RENDEZVOUS_HASHING_ALGORITHM = "RENDEZVOUS";
    private static final String CONSISTENT_HASHING_ALGORITHM = "CONSISTENT";

    private HashingAlgorithmConfigUtils() {
    }

    public static HashingAlgorithm getHashingAlgorithm() {
        String value = System.getenv(ENV_KEY);

        if (value == null || value.isBlank()) {
            return HashingAlgorithm.RENDEZVOUS;
        }

        if (CONSISTENT_HASHING_ALGORITHM.equalsIgnoreCase(value)) {
            return HashingAlgorithm.CONSISTENT;
        }

        if (RENDEZVOUS_HASHING_ALGORITHM.equalsIgnoreCase(value)) {
            return HashingAlgorithm.RENDEZVOUS;
        }

        return HashingAlgorithm.RENDEZVOUS;
    }

}
