ARG METABASE_VERSION=v0.62.19.x
FROM eclipse-temurin:21-jre-jammy

ARG METABASE_VERSION
ARG DRIVER_VERSION=1.5.4.0

ENV MB_PLUGINS_DIR=/plugins

RUN mkdir -p /app /plugins && \
    apt-get update && apt-get install -y --no-install-recommends curl fontconfig ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ADD https://downloads.metabase.com/${METABASE_VERSION}/metabase.jar /app/metabase.jar
ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/${DRIVER_VERSION}/duckdb.metabase-driver.jar /plugins/duckdb.metabase-driver.jar

RUN chmod 744 /plugins/duckdb.metabase-driver.jar

EXPOSE 3000
CMD ["java", "-jar", "/app/metabase.jar"]