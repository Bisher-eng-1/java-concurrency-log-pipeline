# Stage 1: Reproducible Maven build with Java 17
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom.xml first to leverage Docker layer caching for dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build packaged JAR (tests run during CI or explicitly via maven)
COPY src/ ./src/
RUN mvn package -DskipTests -B

# Stage 2: Minimal Java 17 JRE runtime
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Create dedicated non-root user
RUN groupadd -r pipeline && useradd -r -g pipeline -d /app -s /sbin/nologin pipeline

COPY --from=builder /build/target/java-concurrency-log-pipeline.jar /app/java-concurrency-log-pipeline.jar
RUN chown -R pipeline:pipeline /app

USER pipeline

ENTRYPOINT ["java", "-jar", "/app/java-concurrency-log-pipeline.jar"]
CMD ["--mode", "compare", "--workers", "4", "--events", "50000", "--queue-capacity", "1000"]
