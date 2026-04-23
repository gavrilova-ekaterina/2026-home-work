package company.vk.edu.distrib.compute.gavrilova_ekaterina;

import com.sun.net.httpserver.HttpExchange;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class RequestParserHelper {

    private static final String PARAM_ID = "id";

    private RequestParserHelper() {
    }

    public static String extractId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }

        for (String param : query.split("&")) {
            String[] kv = param.split("=");
            if (kv.length == 2 && PARAM_ID.equals(kv[0])) {
                return URLDecoder.decode(kv[1], StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    public static int extractAck(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return 1;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("ack=")) {
                String value = param.substring(4);
                try {
                    return Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    return 1;
                }
            }
        }
        return 1;
    }

}
