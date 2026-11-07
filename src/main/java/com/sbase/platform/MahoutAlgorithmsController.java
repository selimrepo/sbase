package com.sbase.platform;
import jakarta.validation.Valid;import jakarta.validation.constraints.*;import org.springframework.web.bind.annotation.*;import java.util.*;
@RestController @RequestMapping("/api/algorithms/mahout") public class MahoutAlgorithmsController {
 private final MahoutAlgorithmRegistry registry;public MahoutAlgorithmsController(MahoutAlgorithmRegistry r){registry=r;}
 public record Request(@NotBlank String algorithm,@NotNull @Size(min=1) List<Double> left,@NotNull @Size(min=1) List<Double> right){}
 @GetMapping public Set<String> list(){return registry.algorithms();}
 @PostMapping("/score") public Map<String,Double> score(@Valid @RequestBody Request r){return Map.of("score",registry.score(r.algorithm(),r.left().stream().mapToDouble(Double::doubleValue).toArray(),r.right().stream().mapToDouble(Double::doubleValue).toArray()));}
}