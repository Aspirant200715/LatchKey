package io.latchkey.consumer;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/** Retry database and infrastructure failures without acknowledging the record. */
@Configuration
@Profile("consumer")
public class ConsumerErrorHandlerConfiguration {

  @Bean
  ThreadPoolTaskScheduler consumerTaskScheduler() {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(1);
    scheduler.setThreadNamePrefix("consumer-pause-");
    return scheduler;
  }

  @Bean
  CommonErrorHandler kafkaErrorHandler(
      ListenerContainerRegistry registry, TaskScheduler taskScheduler) {
    ListenerContainerPauseService pauseService =
        new ListenerContainerPauseService(registry, taskScheduler);
    DefaultErrorHandler handler =
        new DefaultErrorHandler(
            (record, exception) -> {
              MessageListenerContainer container = registry.getListenerContainer("inventory-consumer");
              if (container != null) {
                pauseService.pausePartition(
                    container,
                    new TopicPartition(record.topic(), record.partition()),
                    Duration.ofSeconds(10));
              }
              // A failed database transaction must never advance the group offset. Leave the
              // record at the head of its partition and retry after the short pause.
              throw new KafkaException("consumer retries exhausted; record remains uncommitted", exception);
            },
            jitteredBackOff(),
            new ContainerPausingBackOffHandler(pauseService));
    handler.setAckAfterHandle(false);
    handler.setCommitRecovered(false);
    return handler;
  }

  private static BackOff jitteredBackOff() {
    return () ->
        new BackOffExecution() {
          private int retries;
          private long ceilingMs = 100;

          @Override
          public long nextBackOff() {
            if (retries++ >= 4) {
              return STOP;
            }
            long delay = ThreadLocalRandom.current().nextLong(ceilingMs + 1);
            ceilingMs = Math.min(10_000, ceilingMs * 2);
            return delay;
          }
        };
  }
}
