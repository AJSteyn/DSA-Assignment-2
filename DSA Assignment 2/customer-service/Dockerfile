FROM ballerina/ballerina:2201.12.0

WORKDIR /app

COPY Ballerina.toml .
COPY Config.toml .
COPY *.bal .

RUN bal build

EXPOSE 8081

CMD ["bal", "run", "target/bin/customer_service.jar"]
