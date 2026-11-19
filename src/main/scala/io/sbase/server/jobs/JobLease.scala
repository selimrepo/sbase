package io.sbase.server.jobs
import java.util.UUID
final case class JobLease(id: UUID, leaseOwner: UUID, attempt: Int, maxAttempts: Int, workflowDefinition: String, inputParameters: String = "{}") extends SBaseSerializable
