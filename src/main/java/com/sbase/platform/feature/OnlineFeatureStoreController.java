package com.sbase.platform.feature;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/api/online-features")
public class OnlineFeatureStoreController {
    private final OnlineFeatureStoreService features;
    private final FeatureAccessService access;
    private final com.sbase.platform.tenant.TenantContext tenants;

    public OnlineFeatureStoreController(OnlineFeatureStoreService features, FeatureAccessService access, com.sbase.platform.tenant.TenantContext tenants) {
        this.features = features;
        this.access = access;
        this.tenants = tenants;
    }

    @PutMapping("/{featureSet}/entities/{entityKey}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void materialize(@PathVariable String featureSet, @PathVariable String entityKey,
                            @Valid @RequestBody UpsertRequest request, Authentication auth) {
        access.require(auth, featureSet, true);
        features.upsert(tenants.tenantId(auth),featureSet,entityKey,request.featureVersion(),request.eventTime(),request.values());
    }

    @GetMapping("/{featureSet}/entities/{entityKey}")
    public Map<String, Object> serve(@PathVariable String featureSet, @PathVariable String entityKey, Authentication auth) {
        access.require(auth, featureSet, false);
        return features.get(tenants.tenantId(auth),featureSet,entityKey)
            .orElseThrow(() -> new org.springframework.web.server.ResponseStatusException(HttpStatus.NOT_FOUND, "Online feature vector not found"));
    }

    @GetMapping("/{featureSet}/_count")
    public CountResponse count(@PathVariable String featureSet, Authentication auth) {
        access.require(auth, featureSet, false);
        return new CountResponse(features.count(tenants.tenantId(auth),featureSet));
    }

    public record UpsertRequest(@Positive int featureVersion, Instant eventTime, @NotEmpty Map<String, Object> values) {}
    public record CountResponse(long entities) {}
}
