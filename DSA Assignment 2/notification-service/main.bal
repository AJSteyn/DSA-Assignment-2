import ballerina/http;
import ballerinax/kafka;
import ballerina/uuid;
import ballerina/time;
import ballerina/log;

configurable string kafkaBootstrapServers = "localhost:9092";
configurable int servicePort = 8086;

public enum NotificationType {
    ORDER_CREATED, ORDER_CONFIRMED, ORDER_PREPARING, ORDER_READY, ORDER_OUT_FOR_DELIVERY, 
    ORDER_DELIVERED, ORDER_CANCELLED, PAYMENT_COMPLETED, PAYMENT_FAILED, DRIVER_ASSIGNED, DELIVERY_UPDATE
}

public enum NotificationChannel {
    EMAIL, SMS, PUSH, IN_APP
}

public enum RecipientType {
    CUSTOMER, RESTAURANT, DRIVER
}

public type Notification record {| 
    string id;
    string recipientId;
    RecipientType recipientType;
    NotificationChannel channel;
    NotificationType notificationType;
    string title;
    string message;
    map<string>? metadata = ();
    boolean isRead;
    string createdAt;
|};

public type NotificationPreference record {| 
    string userId;
    NotificationChannel[] channels;
    NotificationType[] enabledTypes;
|};

public type NotificationSendRequest record {| 
    string recipientId;
    RecipientType recipientType;
    NotificationChannel channel;
    NotificationType notificationType;
    string title;
    string message;
    map<string>? metadata = ();
|};

// In-memory data stores
map<Notification> notificationsStore = {};
map<NotificationPreference> preferencesStore = {};

// HTTP Service
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /notifications on new http:Listener(servicePort) {

    resource function get . (string? recipientId, string? recipientType, boolean? isRead) returns Notification[]|error {
        Notification[] filtered = [];
        foreach var notif in notificationsStore.toArray() {
            boolean matchRecipient = recipientId is () || notif.recipientId == recipientId;
            boolean matchType = recipientType is () || notif.recipientType.toString() == recipientType;
            boolean matchRead = isRead is () || notif.isRead == isRead;
            
            if matchRecipient && matchType && matchRead {
                filtered.push(notif);
            }
        }
        return filtered;
    }

    resource function get [string id]() returns Notification|http:NotFound {
        if notificationsStore.hasKey(id) {
            return notificationsStore.get(id);
        }
        return http:NOT_FOUND;
    }

    resource function put [string id]/read() returns Notification|http:NotFound {
        if notificationsStore.hasKey(id) {
            Notification notif = notificationsStore.get(id);
            notif.isRead = true;
            notificationsStore[id] = notif;
            return notif;
        }
        return http:NOT_FOUND;
    }

    resource function get recipient/[string recipientId]() returns Notification[]|error {
        Notification[] filtered = [];
        foreach var notif in notificationsStore.toArray() {
            if notif.recipientId == recipientId {
                filtered.push(notif);
            }
        }
        return filtered;
    }

    resource function get recipient/[string recipientId]/unread() returns int {
        int count = 0;
        foreach var notif in notificationsStore.toArray() {
            if notif.recipientId == recipientId && !notif.isRead {
                count += 1;
            }
        }
        return count;
    }

    resource function post send(@http:Payload NotificationSendRequest req) returns Notification|error {
        return sendNotification(req.recipientId, req.recipientType, req.channel, req.notificationType, req.title, req.message, req.metadata);
    }

    resource function post preferences(@http:Payload NotificationPreference pref) returns NotificationPreference|error {
        preferencesStore[pref.userId] = pref;
        return pref;
    }

    resource function get preferences/[string userId]() returns NotificationPreference|http:NotFound {
        if preferencesStore.hasKey(userId) {
            return preferencesStore.get(userId);
        }
        return http:NOT_FOUND;
    }
}

function sendNotification(string recipientId, RecipientType recipientType, NotificationChannel channel, NotificationType notificationType, string title, string message, map<string>? metadata) returns Notification {
    Notification notif = {
        id: uuid:createType1AsString(),
        recipientId: recipientId,
        recipientType: recipientType,
        channel: channel,
        notificationType: notificationType,
        title: title,
        message: message,
        metadata: metadata,
        isRead: false,
        createdAt: time:utcToString(time:utcNow())
    };
    notificationsStore[notif.id] = notif;
    log:printInfo(string `[SIMULATED DELIVERY via ${channel}] To: ${recipientId} (${recipientType}) | Title: ${title} | Message: ${message}`);
    return notif;
}

// Kafka Consumer
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "notification-service-group",
    topics: [
        "orders.created", 
        "orders.confirmed", 
        "orders.status-updated", 
        "orders.cancelled", 
        "payments.completed", 
        "payments.failed", 
        "delivery.assigned", 
        "delivery.completed"
    ],
    pollingInterval: 1,
    autoCommit: true
};

listener kafka:Listener kafkaListener = new (kafkaBootstrapServers, consumerConfiguration);

service on kafkaListener {
    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            string topic = kafkaRecord.topic;
            var value = kafkaRecord.value;
            
            if value is string {
                log:printInfo("Received event on topic: " + topic);
                processEvent(topic, value);
            } else {
                log:printError("Unsupported message type received on topic: " + topic);
            }
        }
    }
}

function processEvent(string topic, string payloadStr) {
    // Basic event parsing assuming JSON payload
    do {
        json payload = check value:fromJsonString(payloadStr);
        
        match topic {
            "orders.created" => {
                string customerId = check payload.customerId;
                string restaurantId = check payload.restaurantId;
                string orderId = check payload.orderId;
                
                _ = sendNotification(customerId, CUSTOMER, PUSH, ORDER_CREATED, "Order Received!", string `Your order #${orderId} has been successfully created and sent to the restaurant.`, {"orderId": orderId});
                _ = sendNotification(restaurantId, RESTAURANT, IN_APP, ORDER_CREATED, "New Order Received!", string `You have a new order #${orderId} to prepare.`, {"orderId": orderId});
            }
            "orders.confirmed" => {
                string customerId = check payload.customerId;
                string orderId = check payload.orderId;
                
                _ = sendNotification(customerId, CUSTOMER, PUSH, ORDER_CONFIRMED, "Order Confirmed", string `The restaurant has confirmed your order #${orderId}. Preparation will start soon.`, {"orderId": orderId});
            }
            "orders.status-updated" => {
                string customerId = check payload.customerId;
                string orderId = check payload.orderId;
                string status = check payload.status;
                
                if status == "PREPARING" {
                    _ = sendNotification(customerId, CUSTOMER, IN_APP, ORDER_PREPARING, "Order is being prepared", string `Your order #${orderId} is now being prepared.`, {"orderId": orderId});
                } else if status == "READY" {
                    _ = sendNotification(customerId, CUSTOMER, PUSH, ORDER_READY, "Order Ready", string `Your order #${orderId} is ready for pickup by the driver.`, {"orderId": orderId});
                } else if status == "OUT_FOR_DELIVERY" {
                    _ = sendNotification(customerId, CUSTOMER, SMS, ORDER_OUT_FOR_DELIVERY, "Out for Delivery", string `Your order #${orderId} is on the way!`, {"orderId": orderId});
                }
            }
            "orders.cancelled" => {
                string customerId = check payload.customerId;
                string restaurantId = check payload.restaurantId;
                string orderId = check payload.orderId;
                
                _ = sendNotification(customerId, CUSTOMER, EMAIL, ORDER_CANCELLED, "Order Cancelled", string `Your order #${orderId} has been cancelled.`, {"orderId": orderId});
                _ = sendNotification(restaurantId, RESTAURANT, IN_APP, ORDER_CANCELLED, "Order Cancelled", string `Order #${orderId} was cancelled.`, {"orderId": orderId});
                
                var driverId = payload.driverId;
                if driverId is string && driverId != "" {
                    _ = sendNotification(driverId, DRIVER, PUSH, ORDER_CANCELLED, "Delivery Cancelled", string `The delivery for order #${orderId} has been cancelled.`, {"orderId": orderId});
                }
            }
            "payments.completed" => {
                string customerId = check payload.customerId;
                string orderId = check payload.orderId;
                
                _ = sendNotification(customerId, CUSTOMER, EMAIL, PAYMENT_COMPLETED, "Payment Successful", string `Payment for order #${orderId} has been processed successfully.`, {"orderId": orderId});
            }
            "payments.failed" => {
                string customerId = check payload.customerId;
                string orderId = check payload.orderId;
                
                _ = sendNotification(customerId, CUSTOMER, SMS, PAYMENT_FAILED, "Payment Failed", string `Your payment for order #${orderId} failed. Please update your payment method.`, {"orderId": orderId});
            }
            "delivery.assigned" => {
                string customerId = check payload.customerId;
                string driverId = check payload.driverId;
                string orderId = check payload.orderId;
                
                _ = sendNotification(customerId, CUSTOMER, PUSH, DRIVER_ASSIGNED, "Driver Assigned", string `A driver has been assigned to your order #${orderId}.`, {"orderId": orderId});
                _ = sendNotification(driverId, DRIVER, IN_APP, DRIVER_ASSIGNED, "New Delivery Assigned", string `You have been assigned to deliver order #${orderId}.`, {"orderId": orderId});
            }
            "delivery.completed" => {
                string customerId = check payload.customerId;
                string restaurantId = check payload.restaurantId;
                string orderId = check payload.orderId;
                
                _ = sendNotification(customerId, CUSTOMER, PUSH, ORDER_DELIVERED, "Order Delivered", string `Your order #${orderId} has been delivered. Enjoy your meal!`, {"orderId": orderId});
                _ = sendNotification(restaurantId, RESTAURANT, IN_APP, ORDER_DELIVERED, "Order Fulfilled", string `Order #${orderId} has been successfully delivered to the customer.`, {"orderId": orderId});
            }
            _ => {
                log:printInfo("Unhandled topic: " + topic);
            }
        }
    } on fail error e {
        log:printError("Error processing event: " + e.message());
    }
}
