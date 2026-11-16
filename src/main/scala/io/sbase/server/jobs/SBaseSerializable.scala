package io.sbase.server.jobs

/** Marker trait for messages serialized by the configured Pekko Jackson CBOR binding. */
trait SBaseSerializable extends Serializable
