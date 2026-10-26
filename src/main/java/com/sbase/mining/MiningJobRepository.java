package com.sbase.mining;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface MiningJobRepository extends JpaRepository<MiningJobEntity, UUID> { }
