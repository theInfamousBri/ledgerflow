package io.ledgerflow.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.api.TransactionApiApplication;
import io.ledgerflow.processor.TransactionProcessorApplication;
import io.ledgerflow.provider.PaymentProviderSimulatorApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.Banner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
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
import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class TransactionFlowIT {
    private static final String DATABASE_NAME = "ledgerflow";
    private static final String DATABASE_USERNAME = "ledgerflow";
    private static final String DATABASE_PASSWORD = "ledgerflow-test";
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

    @BeforeAll
    static void startSystem() {
        providerContext = start(PaymentProviderSimulatorApplication.class,
                "--spring.application.name=e2e-provider",
                "--server.port=0",
                "--spring.autoconfigure.exclude=" + NO_DATABASE_AUTOCONFIGURATION,
                "--provider.failure-rate=0.0",
                "--provider.latency-ms=0");
        int providerPort = localPort(providerContext);

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
                "--ledgerflow.provider.base-url=http://localhost:" + providerPort);

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

        var duplicate = postTransaction(idempotencyKey, requestBody);
        assertThat(duplicate.statusCode()).isEqualTo(200);
        JsonNode duplicateBody = JSON.readTree(duplicate.body());
        assertThat(duplicateBody.path("id").asText()).isEqualTo(transactionId);
        assertThat(duplicateBody.path("providerReference").asText())
                .isEqualTo(completed[0].path("providerReference").asText());

        assertDatabaseState(UUID.fromString(transactionId));
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

    private static Iterable<String> statuses(JsonNode transaction) {
        return StreamSupport.stream(transaction.path("history").spliterator(), false)
                .map(item -> item.path("status").asText())
                .toList();
    }

    private static void assertDatabaseState(UUID transactionId) throws Exception {
        try (var connection = DriverManager.getConnection(jdbcUrl(), DATABASE_USERNAME, DATABASE_PASSWORD)) {
            try (var statement = connection.prepareStatement(
                    "SELECT COUNT(*) FROM transactions WHERE id = ?")) {
                statement.setObject(1, transactionId);
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getInt(1)).isEqualTo(1);
                }
            }

            try (var statement = connection.prepareStatement(
                    "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NOT NULL")) {
                statement.setObject(1, transactionId);
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getInt(1)).isEqualTo(1);
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
