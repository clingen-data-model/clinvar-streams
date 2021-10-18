# For more information on these images, and use of Clojure in Docker
# https://hub.docker.com/_/clojure
FROM clojure:openjdk-11-lein

# Copying and building deps as a separate step in order to mitigate
# the need to download new dependencies every build.
WORKDIR /app
COPY project.clj /app/project.clj
COPY src /app/src
COPY resources /app/resources
# TODO remove this from final image
COPY test /app/test
RUN lein deps
RUN lein compile

MAINTAINER Kyle Ferriter <kferrite@broadinstitute.org>

RUN apt-get update && apt-get install sqlite3
EXPOSE 8080
EXPOSE 60001
ENTRYPOINT ["lein", "run", "-m", "clinvar-streams.core-repl"]
