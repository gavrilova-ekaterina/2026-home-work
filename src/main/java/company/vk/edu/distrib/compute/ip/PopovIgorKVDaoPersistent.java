package company.vk.edu.distrib.compute.ip;

import company.vk.edu.distrib.compute.Dao;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class PopovIgorKVDaoPersistent implements Dao<byte[]> {
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final Path storageDir;

    public PopovIgorKVDaoPersistent(String dataPath) throws IOException {
        this.storageDir = Paths.get(dataPath);
        if (!Files.exists(this.storageDir)) {
            Files.createDirectories(this.storageDir);
        }
    }

    @Override
    public byte[] get(String key) throws IOException {
        this.checkActive();
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        final Path file = this.storageDir.resolve(Paths.get(key).getFileName().toString());
        return Files.readAllBytes(file);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        this.checkActive();
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        final Path file = this.storageDir.resolve(Paths.get(key).getFileName().toString());
        Files.write(file, value);
    }

    @Override
    public void delete(String key) throws IOException {
        this.checkActive();
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        final Path file = this.storageDir.resolve(Paths.get(key).getFileName().toString());
        Files.deleteIfExists(file);
    }

    @Override
    public void close() throws IOException {
        this.active.set(false);
    }

    private void checkActive() throws IOException {
        if (!this.active.get()) {
            throw new IOException("DAO is already closed");
        }
    }
}
