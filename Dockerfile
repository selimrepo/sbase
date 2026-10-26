FROM gradle:8.14-jdk21 AS build
WORKDIR /workspace
COPY --chown=gradle:gradle . .
RUN gradle --no-daemon clean bootJar

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 10001 --create-home appuser
COPY --from=build /workspace/build/libs/*.jar /app/sbase.jar
USER appuser
EXPOSE 8080
ENV JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/sbase.jar"]
