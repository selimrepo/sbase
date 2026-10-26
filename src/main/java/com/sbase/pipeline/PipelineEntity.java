package com.sbase.pipeline;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "pipelines")
public class PipelineEntity {
    @Id private UUID id;
    @Column(nullable = false, length = 160) private String name;
    @Column(columnDefinition = "text") private String description;
    @Column(name = "definition_json", nullable = false, columnDefinition = "text") private String definitionJson;
    @Column(nullable = false) private int version;
    @Column(nullable = false) private boolean enabled;
    @Column(name = "created_at", nullable = false) private Instant createdAt;
    @Column(name = "updated_at", nullable = false) private Instant updatedAt;

    protected PipelineEntity() { }
    public PipelineEntity(String name, String description, String definitionJson, boolean enabled) {
        this.id = UUID.randomUUID(); this.name = name; this.description = description;
        this.definitionJson = definitionJson; this.version = 1; this.enabled = enabled;
        this.createdAt = Instant.now(); this.updatedAt = this.createdAt;
    }
    public void update(String name, String description, String definitionJson, boolean enabled) {
        this.name = name; this.description = description; this.definitionJson = definitionJson;
        this.enabled = enabled; this.version++; this.updatedAt = Instant.now();
    }
    public UUID getId() { return id; }
    public String getName() { return name; }
    public String getDescription() { return description; }
    public String getDefinitionJson() { return definitionJson; }
    public int getVersion() { return version; }
    public boolean isEnabled() { return enabled; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}