FROM openjdk:8-jre
MAINTAINER Haystack <haystack@expedia.com>

ENV APP_NAME haystack-parquet-writer
ENV APP_HOME /app/bin
ENV APP_CONF /app/conf
ENV JMXTRANS_AGENT jmxtrans-agent-1.2.6

RUN mkdir -p ${APP_HOME} ${APP_CONF}

COPY target/${APP_NAME}.jar ${APP_HOME}/
COPY docker/start-app.sh ${APP_HOME}/
COPY docker/jmxtrans-agent.xml ${APP_HOME}/
COPY config.yml ${APP_CONF}/

RUN chmod +x ${APP_HOME}/start-app.sh
ADD https://github.com/jmxtrans/jmxtrans-agent/releases/download/${JMXTRANS_AGENT}/${JMXTRANS_AGENT}.jar ${APP_HOME}/

WORKDIR ${APP_HOME}

ENTRYPOINT ["./start-app.sh"]