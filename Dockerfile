FROM gradle:8.14.3-jdk17 AS build
WORKDIR /workspace
COPY . .
RUN gradle --no-daemon clean test installDist

FROM eclipse-temurin:17-jre
WORKDIR /opt/sbase
COPY --from=build /workspace/build/install/sbase-server/ ./
EXPOSE 8080
USER 10001
ENTRYPOINT ["bin/sbase-server"]
