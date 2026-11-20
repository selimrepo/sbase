package io.sbase.server.engines;

import org.junit.jupiter.api.Test;
import java.util.Set;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

class OperatorRegistryCatalogTest {
    @Test
    void exposesEveryRegisteredOperatorWithEditorMetadata() {
        var registry = new OperatorRegistry();
        var catalog = registry.operatorCatalog();
        var names = catalog.stream().map(item -> (String) item.get("name")).collect(Collectors.toSet());

        assertEquals(Set.of(
                "dataset-values", "dataset-rows", "mahout-vector-sum", "mahout-cosine-similarity",
                "mahout-euclidean-distance", "mahout-dot-product", "mahout-manhattan-distance",
                "mahout-normalize", "mahout-top-k", "cassandra-query", "calcite-sql", "spark-sql",
                "flink-submit", "flink-jobs-overview"), names);
        assertTrue(catalog.stream().allMatch(item ->
                item.containsKey("category") && item.containsKey("description")
                        && item.containsKey("requiredConfig") && item.containsKey("optionalConfig")));
    }

    @Test
    void unknownOperatorHasNoDescriptor() {
        assertTrue(new OperatorRegistry().operatorDefinition("not-real").isEmpty());
    }
}
