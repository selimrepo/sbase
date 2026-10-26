package com.sbase.mining;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.UUID;

@RestController
@RequestMapping("/api/mining/jobs")
public class MiningJobController {
    private final MiningJobService jobs;

    public MiningJobController(MiningJobService jobs) { this.jobs = jobs; }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public MiningJob submit(@Valid @RequestBody MiningJobRequest request) {
        return jobs.submit(request);
    }

    @GetMapping("/{id}")
    public MiningJob get(@PathVariable UUID id) {
        MiningJob job = jobs.find(id);
        if (job == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Mining job not found");
        return job;
    }
}
