# For more information on these images, and use of Clojure in Docker
# https://hub.docker.com/_/clojure
FROM clojure:openjdk-11-lein AS clinvar-streams-deps

# Copying and building deps as a separate step in order to mitigate
# the need to download new dependencies every build.
COPY project.clj /app/project.clj
WORKDIR /app
RUN lein deps

FROM clinvar-streams-deps AS builder
COPY src /app/src
COPY resources /app/resources
# copy test dir in so that cloudbuild can run them in this image
COPY test /app/test
RUN lein uberjar

# Using image without lein for deployment.
FROM openjdk:11
LABEL maintainer="Kyle Ferriter <kferrite@broadinstitute.org>"

RUN apt-get update && apt-get install sqlite3
COPY --from=builder /app/target/uberjar/clinvar-streams.jar /app/clinvar-streams.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "-XX:MaxRAMPercentage=50", "/app/clinvar-streams.jar"]
