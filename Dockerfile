FROM gradle:8.14-jdk17 AS build
WORKDIR /workspace
COPY --chown=gradle:gradle . .
RUN gradle --no-daemon clean bootJar \
 && mkdir -p /opt/spark-jars \
 && unzip -j build/libs/*.jar 'BOOT-INF/lib/mysql-connector-j-*.jar' -d /opt/spark-jars

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN useradd --system --uid 10001 --create-home appuser
COPY --from=build /workspace/build/libs/*.jar /app/sbase.jar
COPY --from=build /opt/spark-jars/mysql-connector-j-*.jar /opt/spark-jars/mysql-connector.jar
USER appuser
EXPOSE 8080 7078 7079
ENV JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/sbase.jar"]
