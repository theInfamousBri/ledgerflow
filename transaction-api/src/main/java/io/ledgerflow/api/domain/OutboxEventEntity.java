package io.ledgerflow.api.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEventEntity {
    @Id
    private UUID id;
    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;
    @Column(name = "topic", nullable = false, length = 200)
    private String topic;
    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    @Column(name = "published_at")
    private Instant publishedAt;
    @Column(name = "trace_parent", length = 512)
    private String traceParent;
    @Column(name = "trace_state", columnDefinition = "text")
    private String traceState;

    protected OutboxEventEntity() {}

    public OutboxEventEntity(UUID id, UUID aggregateId, String topic, String payload, Instant createdAt) {
        this(id, aggregateId, topic, payload, createdAt, null, null);
    }

    public OutboxEventEntity(UUID id, UUID aggregateId, String topic, String payload, Instant createdAt,
                             String traceParent, String traceState) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.payload = payload;
        this.createdAt = createdAt;
        this.traceParent = traceParent;
        this.traceState = traceState;
    }

    public void markPublished(Instant at) { this.publishedAt = at; }
    public UUID getId() { return id; }
    public UUID getAggregateId() { return aggregateId; }
    public String getTopic() { return topic; }
    public String getPayload() { return payload; }
    public String getTraceParent() { return traceParent; }
    public String getTraceState() { return traceState; }
}
