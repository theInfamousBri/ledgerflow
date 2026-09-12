package io.ledgerflow.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.api.TransactionApiApplication;
import io.ledgerflow.processor.TransactionProcessorApplication;
import io.ledgerflow.provider.PaymentProviderSimulatorApplication;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.Banner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class TransactionFlowIT {
    private static final String DATABASE_NAME = "ledgerflow";
    private static final String DATABASE_USERNAME = "ledgerflow";
    private static final String DATABASE_PASSWORD = "ledgerflow-test";
    private static final String REQUESTED_TOPIC = "ledgerflow.transaction.requested.v1";
    private static final String REQUESTED_DLT_TOPIC = REQUESTED_TOPIC + "-dlt";
    private static final String NO_DATABASE_AUTOCONFIGURATION = String.join(",",
            "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
            "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
            "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration");

    @Container
    private static final GenericContainer<?> POSTGRES = new GenericContainer<>(
            DockerImageName.parse("postgres:17-alpine"))
            .withEnv("POSTGRES_DB", DATABASE_NAME)
            .withEnv("POSTGRES_USER", DATABASE_USERNAME)
            .withEnv("POSTGRES_PASSWORD", DATABASE_PASSWORD)
            .withExposedPorts(5432);

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    private static ConfigurableApplicationContext providerContext;
    private static ConfigurableApplicationContext processorContext;
    private static ConfigurableApplicationContext apiContext;
    private static String apiBaseUrl;
    private static String providerBaseUrl;

    @BeforeAll
    static void startSystem() {
        providerContext = start(PaymentProviderSimulatorApplication.class,
                "--spring.application.name=e2e-provider",
                "--server.port=0",
                "--spring.autoconfigure.exclude=" + NO_DATABASE_AUTOCONFIGURATION,
                "--provider.failure-rate=0.0",
                "--provider.latency-ms=0");
        int providerPort = localPort(providerContext);
        providerBaseUrl = "http://localhost:" + providerPort;

        processorContext = start(TransactionProcessorApplication.class,
                "--spring.application.name=e2e-processor",
                "--server.port=0",
                "--spring.autoconfigure.exclude=" + NO_DATABASE_AUTOCONFIGURATION,
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                "--spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
                "--spring.kafka.producer.acks=all",
                "--spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "--spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--spring.kafka.consumer.enable-auto-commit=false",
                "--ledgerflow.provider.base-url=" + providerBaseUrl);

        apiContext = start(TransactionApiApplication.class,
                "--spring.application.name=e2e-api",
                "--server.port=0",
                "--spring.datasource.url=" + jdbcUrl(),
                "--spring.datasource.username=" + DATABASE_USERNAME,
                "--spring.datasource.password=" + DATABASE_PASSWORD,
                "--spring.jpa.open-in-view=false",
                "--spring.jpa.hibernate.ddl-auto=validate",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                "--spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
                "--spring.kafka.producer.acks=all",
                "--spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "--spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--ledgerflow.outbox.publish-delay-ms=50");
        apiBaseUrl = "http://localhost:" + localPort(apiContext);
    }

    @AfterAll
    static void stopSystem() {
        close(apiContext);
        close(processorContext);
        close(providerContext);
    }

    @Test
    void completesTransactionAndReturnsOriginalResourceForDuplicateRequest() throws Exception {
        String idempotencyKey = "e2e-" + UUID.randomUUID();
        String requestBody = """
                {
                  "accountId": "acct-e2e",
                  "amount": 125.50,
                  "currency": "USD",
                  "type": "PAYMENT"
                }
                """;

        var created = postTransaction(idempotencyKey, requestBody);
        assertThat(created.statusCode()).isEqualTo(202);

        JsonNode createdBody = JSON.readTree(created.body());
        String transactionId = createdBody.path("id").asText();
        assertThat(transactionId).isNotBlank();
        assertThat(createdBody.path("status").asText()).isEqualTo("PENDING");

        JsonNode completed = awaitCompleted(transactionId);

        var duplicate = postTransaction(idempotencyKey, requestBody);
        assertThat(duplicate.statusCode()).isEqualTo(200);
        JsonNode duplicateBody = JSON.readTree(duplicate.body());
        assertThat(duplicateBody.path("id").asText()).isEqualTo(transactionId);
        assertThat(duplicateBody.path("providerReference").asText())
                .isEqualTo(completed.path("providerReference").asText());

        assertDatabaseState(idempotencyKey, UUID.fromString(transactionId));
    }

    @Test
    void createsOneTransactionWhenIdenticalRequestsArriveConcurrently() throws Exception {
        int requestCount = 10;
        String idempotencyKey = "e2e-concurrent-" + UUID.randomUUID();
        String requestBody = """
                {
                  "accountId": "acct-concurrent",
                  "amount": 84.25,
                  "currency": "USD",
                  "type": "PAYMENT"
                }
                """;
        var ready = new CountDownLatch(requestCount);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(requestCount);

        List<HttpResponse<String>> responses;
        try {
            var requests = java.util.stream.IntStream.range(0, requestCount)
                    .mapToObj(index -> executor.submit(() -> {
                        ready.countDown();
                        if (!start.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Timed out waiting to start concurrent requests");
                        }
                        return postTransaction(idempotencyKey, requestBody);
                    }))
                    .toList();

            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            responses = requests.stream()
                    .map(future -> {
                        try {
                            return future.get(10, TimeUnit.SECONDS);
                        } catch (Exception exception) {
                            throw new IllegalStateException("Concurrent request failed", exception);
                        }
                    })
                    .toList();
        } finally {
            start.countDown();
            executor.shutdownNow();
        }

        List<Integer> statusCodes = responses.stream().map(HttpResponse::statusCode).toList();
        assertThat(statusCodes).containsOnly(200, 202);
        assertThat(statusCodes.stream().filter(status -> status == 202).count()).isEqualTo(1);
        assertThat(statusCodes.stream().filter(status -> status == 200).count()).isEqualTo(requestCount - 1L);

        Set<String> transactionIds = responses.stream()
                .map(HttpResponse::body)
                .map(body -> {
                    try {
                        return JSON.readTree(body).path("id").asText();
                    } catch (Exception exception) {
                        throw new IllegalStateException("Could not parse transaction response", exception);
                    }
                })
                .collect(java.util.stream.Collectors.toSet());
        assertThat(transactionIds).hasSize(1).doesNotContain("");

        String transactionId = transactionIds.iterator().next();
        awaitCompleted(transactionId);
        assertDatabaseState(idempotencyKey, UUID.fromString(transactionId));
    }

    @Test
    void redeliveredRequestedEventDoesNotDuplicateProviderEffectOrHistory() throws Exception {
        String idempotencyKey = "e2e-redelivery-" + UUID.randomUUID();
        String requestBody = """
                {
                  "accountId": "acct-redelivery",
                  "amount": 42.10,
                  "currency": "USD",
                  "type": "PAYMENT"
                }
                """;

        var created = postTransaction(idempotencyKey, requestBody);
        assertThat(created.statusCode()).isEqualTo(202);
        String transactionId = JSON.readTree(created.body()).path("id").asText();
        JsonNode completedBeforeRedelivery = awaitCompleted(transactionId);
        String providerReference = completedBeforeRedelivery.path("providerReference").asText();

        JsonNode providerBeforeRedelivery = awaitProviderRequestCount(transactionId, 1);
        assertThat(providerBeforeRedelivery.path("decision").path("providerReference").asText())
                .isEqualTo(providerReference);

        kafka().send(REQUESTED_TOPIC, transactionId,
                        requestedEventPayload(UUID.fromString(transactionId)))
                .get(5, TimeUnit.SECONDS);

        JsonNode providerAfterRedelivery = awaitProviderRequestCount(transactionId, 2);
        assertThat(providerAfterRedelivery.path("decision").path("providerReference").asText())
                .isEqualTo(providerReference);

        await().pollDelay(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    var providerPayment = getProviderPayment(transactionId);
                    assertThat(providerPayment.statusCode()).isEqualTo(200);
                    assertThat(JSON.readTree(providerPayment.body()).path("requestCount").asInt()).isEqualTo(2);

                    var retrieved = getTransaction(transactionId);
                    assertThat(retrieved.statusCode()).isEqualTo(200);
                    JsonNode body = JSON.readTree(retrieved.body());
                    assertThat(body.path("status").asText()).isEqualTo("COMPLETED");
                    assertThat(body.path("providerReference").asText()).isEqualTo(providerReference);
                    assertThat(statuses(body)).containsExactly("PENDING", "PROCESSING", "COMPLETED");
                });

        assertDatabaseState(idempotencyKey, UUID.fromString(transactionId));
    }

    @Test
    void retriesTransientProviderFailuresWithExponentialBackoff() throws Exception {
        configureNextProviderFailures(2);
        String idempotencyKey = "e2e-retry-" + UUID.randomUUID();
        String requestBody = """
                {
                  "accountId": "acct-retry",
                  "amount": 63.75,
                  "currency": "USD",
                  "type": "PAYMENT"
                }
                """;

        var created = postTransaction(idempotencyKey, requestBody);
        assertThat(created.statusCode()).isEqualTo(202);
        String transactionId = JSON.readTree(created.body()).path("id").asText();

        JsonNode completed = awaitCompleted(transactionId);
        JsonNode providerPayment = awaitProviderRequestCount(transactionId, 3);
        assertThat(providerPayment.path("decision").path("providerReference").asText())
                .isEqualTo(completed.path("providerReference").asText());

        List<Instant> attemptTimes = StreamSupport.stream(
                        providerPayment.path("attemptedAt").spliterator(), false)
                .map(item -> Instant.parse(item.asText()))
                .toList();
        assertThat(attemptTimes).hasSize(3);
        long firstBackoffMillis = Duration.between(attemptTimes.get(0), attemptTimes.get(1)).toMillis();
        long secondBackoffMillis = Duration.between(attemptTimes.get(1), attemptTimes.get(2)).toMillis();
        assertThat(firstBackoffMillis).isGreaterThanOrEqualTo(400);
        assertThat(secondBackoffMillis).isGreaterThanOrEqualTo(900);

        assertThat(completed.path("failureCode").isNull()).isTrue();
        assertThat(statuses(completed)).containsExactly("PENDING", "PROCESSING", "COMPLETED");
        assertDatabaseState(idempotencyKey, UUID.fromString(transactionId));
    }

    @Test
    void routesExhaustedRetriesToDeadLetterTopicAndFailsTransaction() throws Exception {
        configureNextProviderFailures(4);
        String idempotencyKey = "e2e-dlt-" + UUID.randomUUID();
        String requestBody = """
                {
                  "accountId": "acct-dlt",
                  "amount": 91.20,
                  "currency": "USD",
                  "type": "PAYMENT"
                }
                """;

        var created = postTransaction(idempotencyKey, requestBody);
        assertThat(created.statusCode()).isEqualTo(202);
        String transactionId = JSON.readTree(created.body()).path("id").asText();
        String originalRequestedEvent = requestedEventPayload(UUID.fromString(transactionId));

        JsonNode failed = awaitFailed(transactionId);
        JsonNode providerPayment = awaitProviderRequestCount(transactionId, 4);
        assertThat(providerPayment.path("decision").isNull()).isTrue();
        assertThat(providerPayment.path("attemptedAt").size()).isEqualTo(4);

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter(transactionId);
        assertThat(deadLetter.key()).isEqualTo(transactionId);
        assertThat(deadLetter.value()).isEqualTo(originalRequestedEvent);

        assertThat(failed.path("providerReference").isNull()).isTrue();
        assertThat(failed.path("failureCode").asText()).isEqualTo("RETRIES_EXHAUSTED");
        assertThat(statuses(failed)).containsExactly("PENDING", "PROCESSING", "FAILED");
        assertDatabaseState(idempotencyKey, UUID.fromString(transactionId));
    }

    private static ConfigurableApplicationContext start(Class<?> application, String... properties) {
        String[] arguments = new String[properties.length + 2];
        arguments[0] = "--spring.config.name=ledgerflow-e2e";
        arguments[1] = "--spring.main.banner-mode=off";
        System.arraycopy(properties, 0, arguments, 2, properties.length);
        return new SpringApplicationBuilder(application)
                .bannerMode(Banner.Mode.OFF)
                .logStartupInfo(false)
                .run(arguments);
    }

    private static int localPort(ConfigurableApplicationContext context) {
        return context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
    }

    private static HttpResponse<String> postTransaction(String idempotencyKey, String body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(apiBaseUrl + "/transactions"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", idempotencyKey)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> getTransaction(String transactionId) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(apiBaseUrl + "/transactions/" + transactionId))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> getProviderPayment(String transactionId) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(providerBaseUrl + "/provider/payments/" + transactionId))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void configureNextProviderFailures(int attempts) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(
                        providerBaseUrl + "/provider/payments/simulation/fail-next?attempts=" + attempts))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        var response = HTTP.send(request, HttpResponse.BodyHandlers.discarding());
        assertThat(response.statusCode()).isEqualTo(204);
    }

    private static Iterable<String> statuses(JsonNode transaction) {
        return StreamSupport.stream(transaction.path("history").spliterator(), false)
                .map(item -> item.path("status").asText())
                .toList();
    }

    private static JsonNode awaitCompleted(String transactionId) {
        var completed = new JsonNode[1];
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    var retrieved = getTransaction(transactionId);
                    assertThat(retrieved.statusCode()).isEqualTo(200);
                    JsonNode body = JSON.readTree(retrieved.body());
                    assertThat(body.path("status").asText()).isEqualTo("COMPLETED");
                    assertThat(statuses(body)).containsExactly("PENDING", "PROCESSING", "COMPLETED");
                    assertThat(body.path("providerReference").asText()).startsWith("sim-");
                    completed[0] = body;
                });
        return completed[0];
    }

    private static JsonNode awaitFailed(String transactionId) {
        var failed = new JsonNode[1];
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    var retrieved = getTransaction(transactionId);
                    assertThat(retrieved.statusCode()).isEqualTo(200);
                    JsonNode body = JSON.readTree(retrieved.body());
                    assertThat(body.path("status").asText()).isEqualTo("FAILED");
                    assertThat(body.path("failureCode").asText()).isEqualTo("RETRIES_EXHAUSTED");
                    assertThat(statuses(body)).containsExactly("PENDING", "PROCESSING", "FAILED");
                    failed[0] = body;
                });
        return failed[0];
    }

    private static JsonNode awaitProviderRequestCount(String transactionId, int expectedCount) {
        var providerPayment = new JsonNode[1];
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    var retrieved = getProviderPayment(transactionId);
                    assertThat(retrieved.statusCode()).isEqualTo(200);
                    JsonNode body = JSON.readTree(retrieved.body());
                    assertThat(body.path("requestCount").asInt()).isEqualTo(expectedCount);
                    providerPayment[0] = body;
                });
        return providerPayment[0];
    }

    private static String requestedEventPayload(UUID transactionId) throws Exception {
        try (var connection = DriverManager.getConnection(jdbcUrl(), DATABASE_USERNAME, DATABASE_PASSWORD);
             var statement = connection.prepareStatement(
                     "SELECT payload FROM outbox_events WHERE aggregate_id = ? AND topic = ?")) {
            statement.setObject(1, transactionId);
            statement.setString(2, REQUESTED_TOPIC);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                String payload = result.getString(1);
                assertThat(result.next()).isFalse();
                return payload;
            }
        }
    }

    private static ConsumerRecord<String, String> awaitDeadLetter(String transactionId) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-dlt-inspector-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        var deadLetter = new AtomicReference<ConsumerRecord<String, String>>();
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.subscribe(List.of(REQUESTED_DLT_TOPIC));
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> {
                        for (var record : consumer.poll(Duration.ofMillis(250))) {
                            if (transactionId.equals(record.key())) {
                                deadLetter.set(record);
                                return true;
                            }
                        }
                        return false;
                    });
        }
        return deadLetter.get();
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafka() {
        return (KafkaTemplate<String, String>) processorContext.getBean(KafkaTemplate.class);
    }

    private static void assertDatabaseState(String idempotencyKey, UUID transactionId) throws Exception {
        try (var connection = DriverManager.getConnection(jdbcUrl(), DATABASE_USERNAME, DATABASE_PASSWORD)) {
            try (var statement = connection.prepareStatement(
                    "SELECT COUNT(*) FROM transactions WHERE idempotency_key = ? AND id = ?")) {
                statement.setString(1, idempotencyKey);
                statement.setObject(2, transactionId);
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getInt(1)).isEqualTo(1);
                }
            }

            try (var statement = connection.prepareStatement(
                    "SELECT COUNT(*), COUNT(published_at) FROM outbox_events WHERE aggregate_id = ?")) {
                statement.setObject(1, transactionId);
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getInt(1)).isEqualTo(1);
                    assertThat(result.getInt(2)).isEqualTo(1);
                }
            }
        }
    }

    private static String jdbcUrl() {
        return "jdbc:postgresql://%s:%d/%s".formatted(
                POSTGRES.getHost(), POSTGRES.getMappedPort(5432), DATABASE_NAME);
    }

    private static void close(ConfigurableApplicationContext context) {
        if (context != null) context.close();
    }
}
