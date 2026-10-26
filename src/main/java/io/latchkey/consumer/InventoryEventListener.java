package io.latchkey.consumer;

import io.latchkey.fault.FailPoints;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** Acknowledges Kafka only after the inventory database transaction has committed. */
@Component
@Profile("consumer")
public class InventoryEventListener {

  private static final Logger log = LoggerFactory.getLogger(InventoryEventListener.class);

  private final InventoryEventProcessor processor;
  private final FailPoints failPoints;

  public InventoryEventListener(InventoryEventProcessor processor, FailPoints failPoints) {
    this.processor = processor;
    this.failPoints = failPoints;
  }

  @KafkaListener(
      id = "inventory-consumer",
      topics = "${latchkey.consumer.topic:order.created}",
      groupId = "${latchkey.consumer.group-id:latchkey-inventory-v1}")
  public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
    InventoryEventProcessor.Outcome outcome = processor.process(record);
    failPoints.hit(FailPoints.AFTER_COMMIT_BEFORE_ACK);
    acknowledgment.acknowledge();
    log.info(
        "processed record partition={} offset={} outcome={}",
        record.partition(),
        record.offset(),
        outcome);
  }
}
