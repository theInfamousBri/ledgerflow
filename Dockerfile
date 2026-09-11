FROM maven:3.9.11-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /workspace
COPY pom.xml .
COPY transaction-contracts/pom.xml transaction-contracts/pom.xml
COPY transaction-api/pom.xml transaction-api/pom.xml
COPY transaction-processor/pom.xml transaction-processor/pom.xml
COPY payment-provider-simulator/pom.xml payment-provider-simulator/pom.xml
RUN mvn -q -pl "${MODULE}" -am dependency:go-offline
COPY . .
RUN mvn -q -pl "${MODULE}" -am package -DskipTests

FROM eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
COPY --from=build /workspace/${MODULE}/target/${MODULE}-*-exec.jar app.jar
USER 10001
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

