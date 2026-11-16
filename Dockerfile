FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src/ ./src/
RUN mvn package -DskipTests -B

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

RUN groupadd -r pipeline && useradd -r -g pipeline -d /app -s /sbin/nologin pipeline

COPY --from=builder /build/target/java-concurrency-log-pipeline.jar /app/java-concurrency-log-pipeline.jar
RUN chown -R pipeline:pipeline /app

USER pipeline

ENTRYPOINT ["java", "-jar", "/app/java-concurrency-log-pipeline.jar"]
CMD ["--mode", "compare", "--workers", "4", "--events", "50000", "--queue-capacity", "1000"]
