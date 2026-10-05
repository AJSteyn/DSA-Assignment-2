FROM ballerina/ballerina:2201.12.0

WORKDIR /app

COPY Ballerina.toml .
COPY Config.toml .
COPY *.bal .

RUN bal build

EXPOSE 8085

CMD ["bal", "run", "target/bin/delivery_service.jar"]
