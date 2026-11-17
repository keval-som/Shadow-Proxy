# =============================================================================
# Shadow Proxy — Container Image
# Build the fat JAR first:  mvn clean package -DskipTests
# Then run:                 docker compose --profile proxy up -d
# =============================================================================

FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="shadow-proxy"
LABEL description="High-Performance Stateless Protocol-Aware Database Proxy"

# Use an application-specific name: Alpine can already contain the system group 'shadow'.
RUN addgroup -S shadowproxy && adduser -S shadowproxy -G shadowproxy

WORKDIR /app

COPY target/shadow-proxy-1.0.0-SNAPSHOT.jar shadow-proxy.jar

RUN mkdir -p logs && chown -R shadowproxy:shadowproxy /app

USER shadowproxy

EXPOSE 5432

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD nc -z localhost 5432 || exit 1

ENTRYPOINT ["java", \
            "--enable-preview", \
            "-Xms256m", "-Xmx1g", \
            "-XX:+UseZGC", \
            "-Dio.netty.allocator.type=pooled", \
            "-jar", "shadow-proxy.jar"]
