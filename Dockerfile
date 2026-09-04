# Build the Spring Boot jar, then run it on a slim JRE.
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app

COPY --from=build /workspace/target/temporal-showcase-1.0.0-SNAPSHOT.jar /app/app.jar
COPY data /app/data

ENV APP_DATA_DIR=/app/data
ENV TEMPORAL_TARGET=127.0.0.1:7233
ENV SERVER_PORT=8080

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
