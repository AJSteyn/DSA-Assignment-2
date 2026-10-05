import ballerina/http;
import ballerina/uuid;
import ballerina/time;
import ballerina/log;
import ballerinax/kafka;

configurable string dbHost = ?;
configurable int dbPort = ?;
configurable string dbName = ?;
configurable string kafkaBootstrapServers = ?;
configurable int servicePort = ?;

public enum OrderStatus {
    CREATED,
    CONFIRMED,
    PREPARING,
    READY,
    OUT_FOR_DELIVERY,
    DELIVERED,
    CANCELLED
}

public type OrderItem record {|
    string menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
    decimal subtotal;
|};

public type StatusChange record {|
    OrderStatus fromStatus?;
    OrderStatus toStatus;
    string changedAt;
    string changedBy;
    string? reason = ();
|};

public type Order record {|
    readonly string id;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    OrderStatus status;
    string deliveryAddress;
    string? driverId = ();
    string? specialInstructions = ();
    string createdAt;
    string updatedAt;
    StatusChange[] statusHistory;
|};

public type OrderItemRequest record {|
    string menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
|};

public type CreateOrderRequest record {|
    string customerId;
    string restaurantId;
    OrderItemRequest[] items;
    string deliveryAddress;
    string? specialInstructions = ();
|};

public type UpdateStatusRequest record {|
    OrderStatus status;
    string changedBy;
    string? reason = ();
|};

public type CancelOrderRequest record {|
    string changedBy;
    string reason;
|};

isolated map<Order> orderTable = {};

kafka:Producer kafkaProducer = check new (kafka:ProducerConfiguration {
    clientId: "order-service-producer",
    bootstrapServers: kafkaBootstrapServers
});

service /orders on new http:Listener(servicePort, {
    cors: {
        allowOrigins: ["*"],
        allowCredentials: false
    }
}) {

    resource function post .(@http:Payload CreateOrderRequest req) returns Order|error {
        string orderId = uuid:createType1AsString();
        string currentTime = time:utcToString(time:utcNow());
        
        OrderItem[] orderItems = [];
        decimal total = 0.0;
        
        foreach var itemReq in req.items {
            decimal sub = itemReq.unitPrice * <decimal>itemReq.quantity;
            total += sub;
            orderItems.push({
                menuItemId: itemReq.menuItemId,
                name: itemReq.name,
                quantity: itemReq.quantity,
                unitPrice: itemReq.unitPrice,
                subtotal: sub
            });
        }
        
        StatusChange initialStatus = {
            toStatus: CREATED,
            changedAt: currentTime,
            changedBy: req.customerId,
            reason: "Order created"
        };
        
        Order newOrder = {
            id: orderId,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: orderItems,
            totalAmount: total,
            status: CREATED,
            deliveryAddress: req.deliveryAddress,
            specialInstructions: req.specialInstructions,
            createdAt: currentTime,
            updatedAt: currentTime,
            statusHistory: [initialStatus]
        };
        
        lock {
            orderTable[orderId] = newOrder.clone();
        }
        
        check publishEvent("orders.created", newOrder);
        return newOrder;
    }

    resource function get . (string? status, string? customerId, string? restaurantId) returns Order[] {
        Order[] orders = [];
        lock {
            foreach var ord in orderTable {
                boolean matchStatus = status is () || ord.status.toString() == status;
                boolean matchCustomer = customerId is () || ord.customerId == customerId;
                boolean matchRestaurant = restaurantId is () || ord.restaurantId == restaurantId;
                if matchStatus && matchCustomer && matchRestaurant {
                    orders.push(ord.clone());
                }
            }
        }
        return orders;
    }

    resource function get [string id]() returns Order|http:NotFound {
        Order? ord;
        lock {
            ord = orderTable[id];
        }
        if ord is () {
            return http:NOT_FOUND;
        }
        return ord.clone();
    }

    resource function put [string id]/status(@http:Payload UpdateStatusRequest req) returns Order|http:NotFound|http:BadRequest|error {
        lock {
            if !orderTable.hasKey(id) {
                return http:NOT_FOUND;
            }
            
            Order ord = orderTable.get(id);
            OrderStatus currentStatus = ord.status;
            OrderStatus newStatus = req.status;
            
            if !isValidTransition(currentStatus, newStatus) {
                return <http:BadRequest> { body: "Invalid status transition from " + currentStatus.toString() + " to " + newStatus.toString() };
            }
            
            string currentTime = time:utcToString(time:utcNow());
            
            StatusChange change = {
                fromStatus: currentStatus,
                toStatus: newStatus,
                changedAt: currentTime,
                changedBy: req.changedBy,
                reason: req.reason
            };
            
            ord.status = newStatus;
            ord.updatedAt = currentTime;
            ord.statusHistory.push(change);
            
            orderTable[id] = ord.clone();
            
            check publishEvent("orders.status-updated", ord);
            if newStatus == CONFIRMED {
                check publishEvent("orders.confirmed", ord);
            }
            
            return ord.clone();
        }
    }

    resource function put [string id]/cancel(@http:Payload CancelOrderRequest req) returns Order|http:NotFound|http:BadRequest|error {
        lock {
            if !orderTable.hasKey(id) {
                return http:NOT_FOUND;
            }
            
            Order ord = orderTable.get(id);
            if ord.status == DELIVERED || ord.status == CANCELLED {
                return <http:BadRequest> { body: "Cannot cancel order in state " + ord.status.toString() };
            }
            
            string currentTime = time:utcToString(time:utcNow());
            
            StatusChange change = {
                fromStatus: ord.status,
                toStatus: CANCELLED,
                changedAt: currentTime,
                changedBy: req.changedBy,
                reason: req.reason
            };
            
            ord.status = CANCELLED;
            ord.updatedAt = currentTime;
            ord.statusHistory.push(change);
            
            orderTable[id] = ord.clone();
            
            check publishEvent("orders.cancelled", ord);
            check publishEvent("orders.status-updated", ord);
            
            return ord.clone();
        }
    }

    resource function get [string id]/track() returns StatusChange[]|http:NotFound {
        lock {
            if !orderTable.hasKey(id) {
                return http:NOT_FOUND;
            }
            return orderTable.get(id).statusHistory.clone();
        }
    }

    resource function get customer/[string customerId]() returns Order[] {
        Order[] orders = [];
        lock {
            foreach var ord in orderTable {
                if ord.customerId == customerId {
                    orders.push(ord.clone());
                }
            }
        }
        return orders;
    }

    resource function get restaurant/[string restaurantId]() returns Order[] {
        Order[] orders = [];
        lock {
            foreach var ord in orderTable {
                if ord.restaurantId == restaurantId {
                    orders.push(ord.clone());
                }
            }
        }
        return orders;
    }
}

isolated function isValidTransition(OrderStatus currentStatus, OrderStatus newStatus) returns boolean {
    if newStatus == CANCELLED {
        return currentStatus != DELIVERED && currentStatus != CANCELLED;
    }

    match currentStatus {
        CREATED => { return newStatus == CONFIRMED; }
        CONFIRMED => { return newStatus == PREPARING; }
        PREPARING => { return newStatus == READY; }
        READY => { return newStatus == OUT_FOR_DELIVERY; }
        OUT_FOR_DELIVERY => { return newStatus == DELIVERED; }
        _ => { return false; }
    }
}

isolated function publishEvent(string topic, anydata payload) returns error? {
    byte[] serializedPayload = payload.toString().toBytes();
    check kafkaProducer->send({
        topic: topic,
        value: serializedPayload
    });
}

service on new kafka:Listener(kafkaBootstrapServers, {
    groupId: "order-service-group",
    topics: ["payments.completed", "delivery.assigned", "delivery.completed"]
}) {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payload = check string:fromBytes(rec.value);
            json jsonPayload = check payload.fromJsonString();
            string topic = rec.topic;
            
            log:printInfo("Received message on topic: " + topic);
            
            match topic {
                "payments.completed" => {
                    string|error orderIdVal = jsonPayload.orderId;
                    if orderIdVal is string {
                        lock {
                            if orderTable.hasKey(orderIdVal) {
                                Order ord = orderTable.get(orderIdVal);
                                if ord.status == CREATED {
                                    string currentTime = time:utcToString(time:utcNow());
                                    StatusChange change = {
                                        fromStatus: ord.status,
                                        toStatus: CONFIRMED,
                                        changedAt: currentTime,
                                        changedBy: "payment-service",
                                        reason: "Payment completed successfully"
                                    };
                                    ord.status = CONFIRMED;
                                    ord.updatedAt = currentTime;
                                    ord.statusHistory.push(change);
                                    orderTable[orderIdVal] = ord.clone();
                                    check publishEvent("orders.confirmed", ord);
                                    log:printInfo("Order confirmed after payment", orderId = orderIdVal);
                                }
                            }
                        }
                    }
                }
                "delivery.assigned" => {
                    string|error orderIdVal = jsonPayload.orderId;
                    string|error driverIdVal = jsonPayload.driverId;
                    if orderIdVal is string && driverIdVal is string {
                        lock {
                            if orderTable.hasKey(orderIdVal) {
                                Order ord = orderTable.get(orderIdVal);
                                ord.driverId = driverIdVal;
                                orderTable[orderIdVal] = ord.clone();
                                log:printInfo("Driver assigned to order", orderId = orderIdVal, driverId = driverIdVal);
                            }
                        }
                    }
                }
                "delivery.completed" => {
                    string|error orderIdVal = jsonPayload.orderId;
                    if orderIdVal is string {
                        lock {
                            if orderTable.hasKey(orderIdVal) {
                                Order ord = orderTable.get(orderIdVal);
                                if ord.status == OUT_FOR_DELIVERY {
                                    string currentTime = time:utcToString(time:utcNow());
                                    StatusChange change = {
                                        fromStatus: ord.status,
                                        toStatus: DELIVERED,
                                        changedAt: currentTime,
                                        changedBy: "delivery-service",
                                        reason: "Delivery completed"
                                    };
                                    ord.status = DELIVERED;
                                    ord.updatedAt = currentTime;
                                    ord.statusHistory.push(change);
                                    orderTable[orderIdVal] = ord.clone();
                                    log:printInfo("Order delivered", orderId = orderIdVal);
                                }
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
