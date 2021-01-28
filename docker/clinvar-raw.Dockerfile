# For more information on these images, and use of Clojure in Docker
# https://hub.docker.com/_/clojure
FROM clojure:openjdk-11-lein AS builder

# Copying and building deps as a separate step in order to mitigate
# the need to download new dependencies every build.
COPY project.clj /usr/src/app/project.clj
WORKDIR /usr/src/app
RUN lein deps

COPY . /usr/src/app
RUN lein with-profile +clinvar-raw uberjar

# Using image without lein for deployment.
FROM openjdk:11
MAINTAINER Kyle Ferriter <kferrite@broadinstitute.org>

COPY --from=builder /usr/src/app/target/uberjar/clinvar-raw.jar /app/clinvar-raw.jar

EXPOSE 8888

CMD ["java", "-jar", "/app/clinvar-raw.jar"]
