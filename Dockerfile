FROM gradle:8.14.3-jdk17 AS build
WORKDIR /workspace
COPY . .
# Integration tests require the PostgreSQL service provided by CI, not the image build context.
RUN gradle --no-daemon clean installDist

FROM eclipse-temurin:17-jre
WORKDIR /opt/sbase
COPY --from=build /workspace/build/install/sbase-server/ ./
EXPOSE 8080
USER 10001
ENTRYPOINT ["bin/sbase-server"]
