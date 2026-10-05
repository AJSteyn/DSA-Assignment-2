import ballerina/http;
import ballerina/uuid;
import ballerina/time;
import ballerinax/kafka;
import ballerina/log;
import ballerina/lang.'float as math;

configurable string dbHost = "localhost";
configurable int dbPort = 27017;
configurable string dbName = "delivery_db";
configurable string kafkaBootstrapServers = "localhost:9092";
configurable int servicePort = 8085;

public enum DriverStatus {
    AVAILABLE, BUSY, OFFLINE
}

public enum DeliveryStatus {
    PENDING, ASSIGNED, PICKED_UP, IN_TRANSIT, DELIVERED, FAILED
}

public type Location record {|
    float latitude;
    float longitude;
    string updatedAt;
|};

public type Driver record {|
    string id;
    string name;
    string email;
    string phone;
    string vehicleType;
    string licensePlate;
    Location? currentLocation;
    DriverStatus status;
    float rating;
    int totalDeliveries;
    string createdAt;
|};

public type Delivery record {|
    string id;
    string orderId;
    string? driverId;
    Location? restaurantLocation;
    Location? customerLocation;
    DeliveryStatus status;
    int? estimatedTime;
    int? actualTime;
    float? distance;
    string? assignedAt;
    string? pickedUpAt;
    string? deliveredAt;
    string? createdAt;
|};

public type DriverRegistration record {|
    string name;
    string email;
    string phone;
    string vehicleType;
    string licensePlate;
|};

map<Driver> drivers = {};
map<Delivery> deliveries = {};

kafka:Producer kafkaProducer = check new (kafkaBootstrapServers);

service on new kafka:Listener(kafkaBootstrapServers, {
    groupId: "delivery-group",
    topics: ["orders.confirmed", "orders.cancelled"]
}) {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payload = check string:fromBytes(rec.value);
            json jsonPayload = check payload.fromJsonString();
            string topic = rec.topic;
            
            log:printInfo("Received event on topic: " + topic);
            
            match topic {
                "orders.confirmed" => {
                    string|error orderIdVal = jsonPayload.id;
                    string|error restaurantIdVal = jsonPayload.restaurantId;
                    string|error customerIdVal = jsonPayload.customerId;
                    string|error deliveryAddressVal = jsonPayload.deliveryAddress;
                    
                    if orderIdVal is string && restaurantIdVal is string && customerIdVal is string && deliveryAddressVal is string {
                        string deliveryId = uuid:createType1AsString();
                        string currentTime = time:utcToString(time:utcNow());
                        
                        Delivery newDelivery = {
                            id: deliveryId,
                            orderId: orderIdVal,
                            driverId: (),
                            restaurantLocation: {
                                latitude: -22.5597,
                                longitude: 17.0832,
                                updatedAt: currentTime
                            },
                            customerLocation: {
                                latitude: -22.5600,
                                longitude: 17.0850,
                                updatedAt: currentTime
                            },
                            status: PENDING,
                            estimatedTime: 30,
                            actualTime: (),
                            distance: calculateDistance(-22.5597, 17.0832, -22.5600, 17.0850),
                            assignedAt: (),
                            pickedUpAt: (),
                            deliveredAt: (),
                            createdAt: currentTime
                        };
                        
                        deliveries[deliveryId] = newDelivery;
                        log:printInfo("Delivery created for order", orderId = orderIdVal, deliveryId = deliveryId);
                        
                        _ = @strand {thread: "any"} start assignDriver(deliveryId);
                    }
                }
                "orders.cancelled" => {
                    string|error orderIdVal = jsonPayload.id;
                    if orderIdVal is string {
                        foreach var [deliveryId, delivery] in deliveries.entries() {
                            if delivery.orderId == orderIdVal && delivery.status == PENDING {
                                delivery.status = FAILED;
                                deliveries[deliveryId] = delivery;
                                log:printInfo("Delivery cancelled for order", orderId = orderIdVal, deliveryId = deliveryId);
                            }
                        }
                    }
                }
                _ => {
                    log:printInfo("Unhandled topic: " + topic);
                }
            }
        }
    }
}

function calculateDistance(float lat1, float lon1, float lat2, float lon2) returns float {
    float R = 6371.0;
    float dLat = (lat2 - lat1) * math:PI / 180.0;
    float dLon = (lon2 - lon1) * math:PI / 180.0;
    float a = math:sin(dLat / 2.0) * math:sin(dLat / 2.0) +
              math:cos(lat1 * math:PI / 180.0) * math:cos(lat2 * math:PI / 180.0) *
              math:sin(dLon / 2.0) * math:sin(dLon / 2.0);
    float c = 2.0 * math:atan2(math:sqrt(a), math:sqrt(1.0 - a));
    return R * c;
}

function assignDriver(string deliveryId) returns error? {
    Delivery? delivery = deliveries[deliveryId];
    if delivery is () {
        return error("Delivery not found");
    }

    if delivery.restaurantLocation is () {
        return error("Restaurant location missing");
    }

    Location restLoc = <Location>delivery.restaurantLocation;
    string? bestDriverId = ();
    float minDistance = float:Infinity;

    foreach var [id, driver] in drivers.entries() {
        if driver.status == AVAILABLE && driver.currentLocation is Location {
            Location driverLoc = <Location>driver.currentLocation;
            float dist = calculateDistance(restLoc.latitude, restLoc.longitude, driverLoc.latitude, driverLoc.longitude);
            if dist < minDistance {
                minDistance = dist;
                bestDriverId = id;
            }
        }
    }

    if bestDriverId is string {
        delivery.driverId = bestDriverId;
        delivery.status = ASSIGNED;
        delivery.assignedAt = time:utcToString(time:utcNow());
        deliveries[deliveryId] = delivery;
        
        Driver driver = drivers.get(bestDriverId);
        driver.status = BUSY;
        drivers[bestDriverId] = driver;
        
        check kafkaProducer->send({
            topic: "delivery.assigned",
            value: ("{\"deliveryId\":\"" + deliveryId + "\", \"orderId\":\"" + delivery.orderId + "\", \"driverId\":\"" + bestDriverId + "\"}").toBytes()
        });
        log:printInfo("Driver auto-assigned", driverId = bestDriverId, deliveryId = deliveryId);
    }
}

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service / on new http:Listener(servicePort) {

    resource function post drivers(DriverRegistration reg) returns http:Created|error {
        string newId = uuid:createType1AsString();
        Driver newDriver = {
            id: newId,
            name: reg.name,
            email: reg.email,
            phone: reg.phone,
            vehicleType: reg.vehicleType,
            licensePlate: reg.licensePlate,
            currentLocation: (),
            status: OFFLINE,
            rating: 5.0,
            totalDeliveries: 0,
            createdAt: time:utcToString(time:utcNow())
        };
        drivers[newId] = newDriver;
        return <http:Created>{body: newDriver};
    }

    resource function get drivers(string? status) returns Driver[] {
        Driver[] driverList = [];
        foreach var driver in drivers {
            if status is () || driver.status.toString() == status {
                driverList.push(driver);
            }
        }
        return driverList;
    }

    resource function get drivers/[string id]() returns Driver|http:NotFound {
        Driver? driver = drivers[id];
        if driver is Driver {
            return driver;
        }
        return <http:NotFound>{};
    }

    resource function put drivers/[string id](DriverRegistration reg) returns Driver|http:NotFound {
        Driver? driver = drivers[id];
        if driver is Driver {
            driver.name = reg.name;
            driver.email = reg.email;
            driver.phone = reg.phone;
            driver.vehicleType = reg.vehicleType;
            driver.licensePlate = reg.licensePlate;
            drivers[id] = driver;
            return driver;
        }
        return <http:NotFound>{};
    }

    resource function put drivers/[string id]/status(DriverStatus status) returns Driver|http:NotFound {
        Driver? driver = drivers[id];
        if driver is Driver {
            driver.status = status;
            drivers[id] = driver;
            return driver;
        }
        return <http:NotFound>{};
    }

    resource function put drivers/[string id]/location(float latitude, float longitude) returns Driver|http:NotFound {
        Driver? driver = drivers[id];
        if driver is Driver {
            driver.currentLocation = {
                latitude: latitude,
                longitude: longitude,
                updatedAt: time:utcToString(time:utcNow())
            };
            drivers[id] = driver;
            return driver;
        }
        return <http:NotFound>{};
    }

    resource function get deliveries(string? status, string? driverId) returns Delivery[] {
        Delivery[] deliveryList = [];
        foreach var delivery in deliveries {
            boolean isMatch = true;
            if status is string && delivery.status.toString() != status {
                isMatch = false;
            }
            if driverId is string && delivery.driverId != driverId {
                isMatch = false;
            }
            if isMatch {
                deliveryList.push(delivery);
            }
        }
        return deliveryList;
    }

    resource function get deliveries/[string id]() returns Delivery|http:NotFound {
        Delivery? delivery = deliveries[id];
        if delivery is Delivery {
            return delivery;
        }
        return <http:NotFound>{};
    }

    resource function get deliveries/'order/[string orderId]() returns Delivery|http:NotFound {
        foreach var delivery in deliveries {
            if delivery.orderId == orderId {
                return delivery;
            }
        }
        return <http:NotFound>{};
    }

    resource function put deliveries/[string id]/status(DeliveryStatus status) returns Delivery|http:NotFound|error {
        Delivery? delivery = deliveries[id];
        if delivery is Delivery {
            delivery.status = status;
            if status == PICKED_UP {
                delivery.pickedUpAt = time:utcToString(time:utcNow());
                check kafkaProducer->send({
                    topic: "delivery.picked-up",
                    value: ("{\"deliveryId\":\"" + id + "\"}").toBytes()
                });
            } else if status == DELIVERED {
                delivery.deliveredAt = time:utcToString(time:utcNow());
                if delivery.driverId is string {
                    Driver driver = drivers.get(<string>delivery.driverId);
                    driver.status = AVAILABLE;
                    driver.totalDeliveries += 1;
                    drivers[<string>delivery.driverId] = driver;
                }
                check kafkaProducer->send({
                    topic: "delivery.completed",
                    value: ("{\"deliveryId\":\"" + id + "\", \"orderId\":\"" + delivery.orderId + "\"}").toBytes()
                });
            } else if status == FAILED {
                if delivery.driverId is string {
                    Driver driver = drivers.get(<string>delivery.driverId);
                    driver.status = AVAILABLE;
                    drivers[<string>delivery.driverId] = driver;
                }
                check kafkaProducer->send({
                    topic: "delivery.failed",
                    value: ("{\"deliveryId\":\"" + id + "\"}").toBytes()
                });
            }
            deliveries[id] = delivery;
            return delivery;
        }
        return <http:NotFound>{};
    }

    resource function post deliveries/[string id]/assign(string driverId) returns Delivery|http:NotFound|error {
        Delivery? delivery = deliveries[id];
        Driver? driver = drivers[driverId];
        
        if delivery is Delivery && driver is Driver {
            delivery.driverId = driverId;
            delivery.status = ASSIGNED;
            delivery.assignedAt = time:utcToString(time:utcNow());
            deliveries[id] = delivery;
            
            driver.status = BUSY;
            drivers[driverId] = driver;
            
            check kafkaProducer->send({
                topic: "delivery.assigned",
                value: ("{\"deliveryId\":\"" + id + "\", \"orderId\":\"" + delivery.orderId + "\", \"driverId\":\"" + driverId + "\"}").toBytes()
            });
            return delivery;
        }
        return <http:NotFound>{};
    }
}
