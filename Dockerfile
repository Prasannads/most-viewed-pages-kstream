FROM openjdk:8
RUN apt-get update
RUN apt-get install -y netcat
RUN apt-get install -y jq
RUN apt-get install -y telnet
COPY target/scala-2.12/most-viewed-pages-kstream.jar /usr/lib/most-viewed-pages-kstream.jar
COPY ./docker-local-setup/connector-configuration/ /usr/bin/connector-configuration/
COPY ./docker-local-setup/schemas/ /usr/bin/schemas/
COPY ./docker-local-setup/create-schemas-topics-and-connectors.sh /usr/bin/create-schemas-topics-and-connectors.sh
COPY start.sh /usr/bin/most-viewed-pages-kstream-run.sh
CMD ["/usr/bin/most-viewed-pages-kstream-run.sh"]