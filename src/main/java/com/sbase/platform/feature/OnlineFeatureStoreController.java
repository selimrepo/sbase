package com.sbase.platform.feature;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/online-features")
public class OnlineFeatureStoreController {
    private final OnlineFeatureStoreService features;

    public OnlineFeatureStoreController(OnlineFeatureStoreService features) {
        this.features = features;
    }

    @PutMapping("/{featureSet}/entities/{entityKey}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void materialize(@PathVariable String featureSet, @PathVariable String entityKey,
                            @Valid @RequestBody UpsertRequest request) {
        features.upsert(featureSet, entityKey, request.featureVersion(),
            request.eventTime(), request.values());
    }

    @GetMapping("/{featureSet}/entities/{entityKey}")
    public Map<String, Object> serve(@PathVariable String featureSet, @PathVariable String entityKey) {
        return features.get(featureSet, entityKey)
            .orElseThrow(() -> new org.springframework.web.server.ResponseStatusException(
                HttpStatus.NOT_FOUND, "Online feature vector not found"));
    }

    @GetMapping("/{featureSet}/_count")
    public CountResponse count(@PathVariable String featureSet) {
        return new CountResponse(features.count(featureSet));
    }

    public record UpsertRequest(@Positive int featureVersion, Instant eventTime,
                                @NotEmpty Map<String, Object> values) {}
    public record CountResponse(int entities) {}
}
