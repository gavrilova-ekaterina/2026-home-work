package company.vk.edu.distrib.compute.nesterukia;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryKVDao implements Dao<byte[]> {
    private static final Logger log = LoggerFactory.getLogger(InMemoryKVDao.class);

    private static final Map<String, byte[]> IN_MEMORY_STORAGE = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        byte[] value = IN_MEMORY_STORAGE.get(key);
        if (value == null) {
            log.debug("No element with key='{}' was found in InMemory storage.", key);
            throw new NoSuchElementException();
        } else {
            log.debug("Get from InMemory storage: {}={}", key, value);
            return value;
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        IN_MEMORY_STORAGE.put(key, value);
        log.debug("Upsert to InMemory storage: {}={}", key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        IN_MEMORY_STORAGE.remove(key);
        log.debug("Deleted from InMemory storage by key: {}", key);
    }

    @Override
    public void close() throws IOException {
        // No need to close InMemoryStorage
    }
}
