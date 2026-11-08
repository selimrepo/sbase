package com.sbase.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@Component
public class PipelineDefinitionValidator {
    private static final Set<String> TYPES = Set.of("doris_read", "filter", "select", "with_column", "aggregate", "doris_write", "join", "feature_store_write");
    private static final Set<String> OPERATORS = Set.of("eq", "ne", "gt", "gte", "lt", "lte", "contains", "is_null", "not_null", "in");
    private static final Set<String> COLUMN_OPS = Set.of("cast_double", "lower", "upper", "trim", "abs", "log1p");
    private static final String IDENTIFIER = "[A-Za-z_][A-Za-z0-9_]*";
    private final ObjectMapper mapper;

    public PipelineDefinitionValidator(ObjectMapper mapper) { this.mapper = mapper; }

    public JsonNode parseAndValidate(String json) {
        try {
            JsonNode root = mapper.readTree(json);
            if (root == null || !root.isObject() || !root.path("steps").isArray() || root.path("steps").isEmpty())
                throw invalid("Pipeline must be an object with a non-empty steps array");
            Map<String, JsonNode> steps = new LinkedHashMap<>();
            for (JsonNode step : root.path("steps")) {
                if (!step.isObject()) throw invalid("Each step must be an object");
                String id = text(step, "id");
                String type = text(step, "type");
                if (!id.matches(IDENTIFIER)) throw invalid("Step id must be a simple identifier");
                if (!TYPES.contains(type)) throw invalid("Unsupported step type: " + type);
                if (steps.putIfAbsent(id, step) != null) throw invalid("Duplicate step id: " + id);
                validateStep(step, type);
            }
            Map<String, Integer> marks = new HashMap<>();
            for (String id : steps.keySet()) visit(id, steps, marks);
            return root;
        } catch (ResponseStatusException ex) {
            throw ex;
        } catch (Exception ex) {
            throw invalid("Invalid pipeline JSON: " + ex.getMessage());
        }
    }

    private void validateStep(JsonNode step, String type) {
        switch (type) {
            case "doris_read" -> table(text(step, "table"));
            case "filter" -> {
                identifier(text(step, "column"));
                String op = text(step, "operator");
                if (!OPERATORS.contains(op)) throw invalid("Unsupported filter operator: " + op);
                if (!Set.of("is_null", "not_null").contains(op) && !step.has("value"))
                    throw invalid("Filter step requires value");
                if ("in".equals(op) && (!step.path("value").isArray() || step.path("value").isEmpty()))
                    throw invalid("The in operator requires a non-empty value array");
            }
            case "select" -> {
                JsonNode columns = step.path("columns");
                if (!columns.isArray() || columns.isEmpty()) throw invalid("Select requires a non-empty columns array");
                columns.forEach(c -> identifier(c.asText()));
            }
            case "with_column" -> {
                identifier(text(step, "column"));
                identifier(text(step, "sourceColumn"));
                if (!COLUMN_OPS.contains(text(step, "operation"))) throw invalid("Unsupported column operation");
            }
            case "feature_store_write" -> {
                String featureSet = text(step, "featureSet");
                if (!featureSet.matches("[A-Za-z0-9_.-]{1,160}")) throw invalid("Invalid feature set name");
                identifier(text(step, "entityKeyColumn"));
                if (step.hasNonNull("eventTimeColumn")) identifier(text(step, "eventTimeColumn"));
                if (step.path("featureVersion").asInt(1) < 1) throw invalid("featureVersion must be positive");
            }
            case "join" -> {
                identifier(text(step, "leftKey"));
                identifier(text(step, "rightKey"));
                if (!Set.of("inner", "left", "right", "full").contains(text(step, "joinType")))
                    throw invalid("Join type must be inner, left, right or full");
            }
            case "aggregate" -> {
                JsonNode groups = step.path("groupBy");
                JsonNode metrics = step.path("metrics");
                if (!groups.isArray() || !metrics.isArray() || metrics.isEmpty())
                    throw invalid("Aggregate requires groupBy and non-empty metrics arrays");
                groups.forEach(c -> identifier(c.asText()));
                for (JsonNode metric : metrics) {
                    identifier(text(metric, "column"));
                    identifier(text(metric, "alias"));
                    if (!Set.of("count", "sum", "avg", "min", "max").contains(text(metric, "function")))
                        throw invalid("Unsupported aggregate function");
                }
            }
            case "doris_write" -> {
                table(text(step, "table"));
                String mode = step.path("mode").asText("append");
                if (!"append".equals(mode)) throw invalid("Idempotent Doris writes support append into UNIQUE KEY tables only");
                JsonNode keys = step.path("idempotencyColumns");
                if (!keys.isArray() || keys.isEmpty()) throw invalid("Doris write requires non-empty idempotencyColumns");
                keys.forEach(key -> identifier(key.asText()));
            }
            default -> throw invalid("Unsupported step type: " + type);
        }
        JsonNode deps = step.path("dependsOn");
        if (!deps.isMissingNode()) {
            if (!deps.isArray() || deps.size() > ("join".equals(type) ? 2 : 1))
                throw invalid("dependsOn must contain at most two steps; only join accepts two inputs");
            if ("join".equals(type) && deps.size() != 2)
                throw invalid("Join requires exactly two dependencies");
            if (!"join".equals(type) && deps.size() == 2)
                throw invalid("Only join steps may have two dependencies");
            for (JsonNode dep : deps) identifier(dep.asText());
        }
    }

    private void visit(String id, Map<String, JsonNode> steps, Map<String, Integer> marks) {
        int mark = marks.getOrDefault(id, 0);
        if (mark == 1) throw invalid("Pipeline contains a dependency cycle at step: " + id);
        if (mark == 2) return;
        marks.put(id, 1);
        for (JsonNode dep : steps.get(id).path("dependsOn")) {
            String dependency = dep.asText();
            if (!steps.containsKey(dependency)) throw invalid("Unknown dependency: " + dependency);
            visit(dependency, steps, marks);
        }
        marks.put(id, 2);
    }

    public static void identifier(String value) {
        if (value == null || !value.matches(IDENTIFIER)) throw invalid("Unsafe or invalid identifier: " + value);
    }

    public static void table(String value) {
        if (value == null || !value.matches(IDENTIFIER + "(\\." + IDENTIFIER + ")?"))
            throw invalid("Unsafe or invalid table identifier: " + value);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank())
            throw invalid("Step requires a non-empty string field: " + field);
        return value.asText();
    }

    private static ResponseStatusException invalid(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }
}
