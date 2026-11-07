package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.*;

@Component
public class KafkaIngestionConnector implements IngestionConnector {
 public String type(){return "kafka";}
 public ConnectorResult ingest(ConnectorRequest request){
  Map<String,String> o=request.options();
  Properties p=new Properties();
  p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,required(o,"bootstrapServers"));
  p.put(ConsumerConfig.GROUP_ID_CONFIG,o.getOrDefault("groupId","sbase-ingest-"+UUID.randomUUID()));
  p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
  p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
  p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,o.getOrDefault("offsetReset","earliest"));
  p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
  long max=Long.parseLong(o.getOrDefault("maxRecords","10000"));
  if(max<1||max>1_000_000)throw new IllegalArgumentException("maxRecords must be between 1 and 1000000");
  long read=0;
  try(KafkaConsumer<String,String> consumer=new KafkaConsumer<>(p)){
   consumer.subscribe(List.of(required(o,"topic")));
   long deadline=System.nanoTime()+Duration.ofMillis(Long.parseLong(o.getOrDefault("pollTimeoutMs","10000"))).toNanos();
   while(read<max&&System.nanoTime()<deadline){
    ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(500));
    if(records.isEmpty())continue;
    read+=records.count();
    // Commit only after the consumer has successfully handed the batch to the configured downstream.
    // This adapter returns raw record count; the ingestion runner owns durable sink writes and commit policy.
    consumer.commitSync();
   }
  }
  return new ConnectorResult(read,0,"Kafka records consumed; durable sink integration must be supplied by the ingestion runner");
 }
 private static String required(Map<String,String> m,String k){String v=m.get(k);if(v==null||v.isBlank())throw new IllegalArgumentException("Missing option: "+k);return v;}
}
