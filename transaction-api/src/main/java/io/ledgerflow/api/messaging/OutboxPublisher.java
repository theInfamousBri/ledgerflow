package io.ledgerflow.api.messaging;

import io.ledgerflow.api.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxMetrics metrics;
    private final Clock clock;

    public OutboxPublisher(OutboxEventRepository repository,
                           KafkaTemplate<String, String> kafka,
                           OutboxMetrics metrics,
                           Clock clock) {
        this.repository = repository;
        this.kafka = kafka;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${ledgerflow.outbox.publish-delay-ms:500}")
    public void publishBatch() {
        for (var event : repository.findByPublishedAtIsNullOrderByCreatedAtAsc(PageRequest.of(0, 100))) {
            try {
                kafka.send(event.getTopic(), event.getAggregateId().toString(), event.getPayload()).get(5, TimeUnit.SECONDS);
                event.markPublished(Instant.now(clock));
                repository.save(event);
                metrics.recordPublished();
            } catch (Exception exception) {
                metrics.recordPublishFailure();
                log.warn("Outbox publish failed eventId={} transactionId={}",
                        event.getId(), event.getAggregateId(), exception);
                return;
            }
        }
    }
}

