package company.vk.edu.distrib.compute;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Contains utility methods for unit tests.
 *
 */
abstract class TestBase {
    private static final int VALUE_LENGTH = 1024;

    static int randomPort() {
        final var port = ThreadLocalRandom.current().nextInt(10000, 60000);
        for (int i = 0; i < 100_000; i++) {
            if (isTcpPortAvailable(port)) {
                return port;
            }
        }
        throw new IllegalStateException("Can't find available port");
    }

    static boolean isTcpPortAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    static String randomKey() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    static String endpoint(int port) {
        return "http://localhost:" + port;
    }
}
