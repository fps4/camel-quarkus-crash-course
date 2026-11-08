# Multi-stage build: compiles inside Docker, so the Docker host needs no JDK or Maven.
# Built by ../00-lab-setup/docker-compose.yml (profile "app"). JVM mode; native is a module 07 exercise.
FROM maven:3.9.16-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
# Cache dependencies in their own layer; a code change then rebuilds in seconds.
RUN mvn -B -q -e dependency:go-offline || true
COPY src ./src
RUN mvn -B package -DskipTests

FROM eclipse-temurin:21.0.13_9-jre
WORKDIR /app
# Quarkus "fast-jar" layout: lib/, app/, quarkus/ + quarkus-run.jar
COPY --from=build /src/target/quarkus-app/ /app/
EXPOSE 8080
USER 1001
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
CMD ["sh", "-c", "exec java $JAVA_OPTS -jar /app/quarkus-run.jar"]
