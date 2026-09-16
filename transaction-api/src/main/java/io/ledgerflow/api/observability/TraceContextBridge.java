package io.ledgerflow.api.observability;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class TraceContextBridge {
    private static final String TRACE_PARENT = "traceparent";
    private static final String TRACE_STATE = "tracestate";
    private static final TextMapSetter<Map<String, String>> SETTER = Map::put;
    private static final TextMapSetter<Headers> KAFKA_SETTER = (headers, key, value) -> {
        headers.remove(key);
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    };
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    };

    private final OpenTelemetry openTelemetry;
    private final io.opentelemetry.api.trace.Tracer tracer;

    public TraceContextBridge(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer("io.ledgerflow.transaction-api");
    }

    public PersistedTraceContext capture() {
        var carrier = new HashMap<String, String>();
        openTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), carrier, SETTER);
        return new PersistedTraceContext(carrier.get(TRACE_PARENT), carrier.get(TRACE_STATE));
    }

    public Optional<String> currentTraceId() {
        var spanContext = Span.current().getSpanContext();
        return spanContext.isValid() ? Optional.of(spanContext.getTraceId()) : Optional.empty();
    }

    public void injectCurrent(Headers headers) {
        openTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), headers, KAFKA_SETTER);
    }

    public void runWithParent(PersistedTraceContext persisted,
                              String spanName,
                              CheckedRunnable action) throws Exception {
        Context parent = extract(persisted);
        Span span = tracer.spanBuilder(spanName).setParent(parent).startSpan();
        try (Scope ignored = span.makeCurrent();
             MDC.MDCCloseable traceId = MDC.putCloseable("traceId", span.getSpanContext().getTraceId());
             MDC.MDCCloseable spanId = MDC.putCloseable("spanId", span.getSpanContext().getSpanId())) {
            action.run();
        } catch (Exception exception) {
            span.recordException(exception);
            span.setStatus(StatusCode.ERROR);
            throw exception;
        } finally {
            span.end();
        }
    }

    private Context extract(PersistedTraceContext persisted) {
        if (persisted == null || persisted.traceParent() == null || persisted.traceParent().isBlank()) {
            return Context.root();
        }
        var carrier = new HashMap<String, String>();
        carrier.put(TRACE_PARENT, persisted.traceParent());
        if (persisted.traceState() != null && !persisted.traceState().isBlank()) {
            carrier.put(TRACE_STATE, persisted.traceState());
        }
        return openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.root(), carrier, GETTER);
    }

    public record PersistedTraceContext(String traceParent, String traceState) {}

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }
}
