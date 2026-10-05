FROM ballerina/ballerina:2201.12.0

WORKDIR /app

COPY Ballerina.toml .
COPY Config.toml .
COPY *.bal .

RUN bal build

EXPOSE 8086

CMD ["bal", "run", "target/bin/notification_service.jar"]
