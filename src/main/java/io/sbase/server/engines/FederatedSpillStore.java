package io.sbase.server.engines;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Stages source rows as JSONL so Spark can scan partitioned files and spill shuffle/sort state to disk. */
public final class FederatedSpillStore {
    private static final ObjectMapper JSON = new ObjectMapper();
    private FederatedSpillStore() {}

    public static Path stage(List<Map<String,Object>> rows) throws Exception {
        Path root = Path.of(System.getenv().getOrDefault("SBASE_FEDERATED_SPILL_PATH",
                System.getProperty("java.io.tmpdir") + "/sbase-federation")).toAbsolutePath().normalize();
        Files.createDirectories(root);
        Path file = Files.createTempFile(root, "sbase-federated-", ".jsonl");
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Map<String,Object> row : rows) {
                writer.write(JSON.writeValueAsString(row));
                writer.newLine();
            }
        } catch (Exception error) {
            Files.deleteIfExists(file);
            throw error;
        }
        file.toFile().deleteOnExit();
        return file;
    }

    public static void cleanup(Collection<Path> files) {
        for (Path file : files) {
            try { Files.deleteIfExists(file); } catch (Exception ignored) { file.toFile().deleteOnExit(); }
        }
    }
}
