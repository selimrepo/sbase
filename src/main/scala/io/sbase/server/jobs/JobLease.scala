package io.sbase.server.jobs
import java.util.UUID
import org.apache.pekko.serialization.jackson.JacksonJsonSerializable
final case class JobLease(id: UUID, leaseOwner: UUID, attempt: Int, maxAttempts: Int, workflowDefinition: String) extends JacksonJsonSerializable
