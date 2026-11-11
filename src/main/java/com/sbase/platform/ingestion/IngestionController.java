package com.sbase.platform.ingestion;
import jakarta.validation.Valid;import jakarta.validation.constraints.*;
import org.springframework.http.HttpStatus;import org.springframework.security.core.Authentication;import org.springframework.web.bind.annotation.*;
import java.util.*;
@RestController @RequestMapping("/api/platform/ingestion")
public class IngestionController {
 private final ConnectorExecutionService service; private final DeadLetterManagementService deadLetters; private final com.sbase.platform.security.PermissionService permissions;
 public IngestionController(ConnectorExecutionService service,DeadLetterManagementService deadLetters,com.sbase.platform.security.PermissionService permissions){this.service=service;this.deadLetters=deadLetters;this.permissions=permissions;}
 public record Request(UUID connectorId,UUID datasetId,String source,String target,@NotEmpty Map<String,String> options,@Size(max=200) String idempotencyKey){}
 @GetMapping("/connectors") public Set<String> connectors(){return service.types();}
 @GetMapping("/dead-letters") public List<Map<String,Object>> deadLetters(@RequestParam(defaultValue="100") int limit,Authentication auth){
  permissions.require(auth,"ingestion:read");
  return deadLetters.pending(limit);
 }
 @PostMapping("/dead-letters/{id}/replay") @ResponseStatus(HttpStatus.ACCEPTED)
 public Map<String,Object> replayDeadLetter(@PathVariable UUID id,Authentication auth){
  permissions.require(auth,"ingestion:write");
  return deadLetters.replay(id);
 }
 @PostMapping("/{type}/runs") @ResponseStatus(HttpStatus.ACCEPTED)
 public Map<String,Object> run(@PathVariable String type,@Valid @RequestBody Request r,Authentication auth){
  permissions.require(auth,"ingestion:write");
  return service.execute(type,r.connectorId(),r.datasetId(),r.source(),r.target(),r.options(),r.idempotencyKey());
 }
}
