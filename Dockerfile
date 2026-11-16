FROM gradle:8.14.3-jdk17 AS build
WORKDIR /workspace
COPY . .
# Integration tests require the PostgreSQL service provided by CI, not the image build context.
RUN gradle --no-daemon clean installDist

FROM eclipse-temurin:17-jre
WORKDIR /opt/sbase
COPY --from=build /workspace/build/install/sbase-server/ ./
RUN mkdir -p /var/lib/sbase/models /var/lib/sbase/spark-tmp \
    && chown -R 10001:10001 /var/lib/sbase
EXPOSE 8080 25520
USER 10001
ENTRYPOINT ["bin/sbase-server"]
