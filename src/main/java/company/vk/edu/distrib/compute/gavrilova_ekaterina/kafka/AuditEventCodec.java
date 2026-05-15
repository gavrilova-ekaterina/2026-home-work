package company.vk.edu.distrib.compute.gavrilova_ekaterina.kafka;

import company.vk.edu.distrib.compute.AuditEvent;

import java.util.Base64;

final class AuditEventCodec {

    private static final String DELIMITER = ":";
    private static final int PARTS_COUNT = 3;
    private static final int METHOD_INDEX = 0;
    private static final int ID_INDEX = 1;
    private static final int TIMESTAMP_INDEX = 2;

    private AuditEventCodec() {
    }

    static String serialize(AuditEvent event) {
        return encode(event.method())
                + DELIMITER
                + encode(event.id())
                + DELIMITER
                + event.timestamp();
    }

    static AuditEvent deserialize(String value) {
        String[] parts = value.split(DELIMITER, PARTS_COUNT);
        if (parts.length != PARTS_COUNT) {
            throw new IllegalArgumentException("Malformed audit event");
        }
        return new AuditEvent(
                decode(parts[METHOD_INDEX]),
                decode(parts[ID_INDEX]),
                Long.parseLong(parts[TIMESTAMP_INDEX])
        );
    }

    private static String encode(String value) {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), java.nio.charset.StandardCharsets.UTF_8);
    }

}
