package company.vk.edu.distrib.compute.gavrilova_ekaterina.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AuditServiceImpl implements AuditService {

    private static final String AUDIT_TOPIC = "audit";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final int STOP_TIMEOUT_SECONDS = 5;

    private final String bootstrapServers;
    private final Path storageFile;
    private final List<AuditEvent> storage = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private boolean loadedFromDisk;
    private String consumerGroupId;
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executor;

    public AuditServiceImpl(String bootstrapServers, String consumerGroupId) throws IOException {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        storageFile = Files.createTempFile("audit-" + sanitize(consumerGroupId) + '-', ".log");
    }

    @Override
    public synchronized void start() {
        startConsumer(consumerGroupId);
    }

    private synchronized void startConsumer(String consumerGroupId) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        this.consumerGroupId = consumerGroupId;
        loadFromDiskOnce();
        consumer = new KafkaConsumer<>(consumerProperties(consumerGroupId));
        consumer.subscribe(List.of(AUDIT_TOPIC));
        executor = Executors.newSingleThreadExecutor();
        executor.submit(this::pollLoop);
    }

    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (consumer != null) {
            consumer.wakeup();
        }
        if (executor != null) {
            executor.shutdown();
            awaitExecutorStop();
            executor = null;
        }
        consumer = null;
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return List.copyOf(storage);
    }

    private Properties consumerProperties(String consumerGroupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void pollLoop() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (!records.isEmpty()) {
                    persist(records);
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            if (running.get()) {
                throw ignored;
            }
        } finally {
            consumer.close();
        }
    }

    private void persist(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            AuditEvent event = AuditEventCodec.deserialize(record.value());
            storage.add(event);
            appendToDisk(event);
        }
    }

    private void loadFromDiskOnce() {
        if (loadedFromDisk) {
            return;
        }
        loadedFromDisk = true;
        if (!Files.exists(storageFile)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(storageFile)) {
            String line = reader.readLine();
            while (line != null) {
                storage.add(AuditEventCodec.deserialize(line));
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load audit log", e);
        }
    }

    private void appendToDisk(AuditEvent event) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                storageFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )) {
            writer.write(AuditEventCodec.serialize(event));
            writer.newLine();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to persist audit event", e);
        }
    }

    private void awaitExecutorStop() {
        try {
            if (!executor.awaitTermination(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private static String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9.-]", "_");
    }

}
