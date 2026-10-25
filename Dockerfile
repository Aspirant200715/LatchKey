# One image, several roles: SPRING_PROFILES_ACTIVE selects api | relay | consumer | cli.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src src
RUN mvn -B -q -DskipTests package \
 && java -Djarmode=tools -jar target/latchkey-*.jar extract --layers --launcher --destination /build/layers

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S latchkey && adduser -S -G latchkey latchkey
WORKDIR /app
# Layers ordered from least to most frequently changed, so rebuilds reuse the cache.
COPY --from=build /build/layers/dependencies/ ./
COPY --from=build /build/layers/spring-boot-loader/ ./
COPY --from=build /build/layers/snapshot-dependencies/ ./
COPY --from=build /build/layers/application/ ./
USER latchkey

# Small, predictable heap: SerialGC, 70% of the container limit, small thread stacks.
ENV JAVA_OPTS="-XX:+UseSerialGC -XX:MaxRAMPercentage=70 -Xss512k -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080 8081
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
