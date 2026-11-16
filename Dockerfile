FROM sbtscala/scala-sbt:eclipse-temurin-17.0.14_7_1.10.11_2.13.16 AS build
WORKDIR /workspace
COPY . .
RUN sbt -batch clean test stage

FROM eclipse-temurin:17-jre
WORKDIR /opt/sbase
COPY --from=build /workspace/target/universal/stage/ ./
EXPOSE 8080
USER 10001
ENTRYPOINT ["bin/sbase-server"]
