import ballerina/http;
import ballerinax/kafka;
import ballerina/uuid;
import ballerina/time;
import ballerina/log;
import ballerina/lang.runtime;
import ballerina/random;

configurable string dbHost = "mongodb";
configurable int dbPort = 27017;
configurable string dbName = "payment_db";
configurable string kafkaBootstrapServers = "kafka:9092";
configurable int servicePort = 8084;

public enum PaymentStatus {
    PENDING, PROCESSING, COMPLETED, FAILED, REFUNDED
}

public enum PaymentMethod {
    CREDIT_CARD, DEBIT_CARD, MOBILE_MONEY, CASH_ON_DELIVERY
}

public type Payment record {|
    readonly string id;
    string orderId;
    string customerId;
    decimal amount;
    string currency = "NAD";
    PaymentMethod method;
    PaymentStatus status;
    string? transactionRef = ();
    string createdAt;
    string updatedAt;
    string? processedAt = ();
|};

public type PaymentRequest record {|
    string orderId;
    string customerId;
    decimal amount;
    PaymentMethod method;
|};

public type RefundRequest record {|
    string paymentId;
    string reason;
|};

public type OrderEvent record {|
    string orderId;
    string customerId;
    decimal totalAmount;
    string status;
    string createdAt;
|};

public type PaymentEvent record {|
    string paymentId;
    string orderId;
    decimal amount;
|};

map<Payment> payments = {};
map<Payment[]> paymentsByOrderId = {};

kafka:Producer kafkaProducer = check new (kafkaBootstrapServers, {
    clientId: "payment-service-producer",
    acks: "all",
    retryCount: 3
});

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service / on new http:Listener(servicePort) {

    resource function post payments(PaymentRequest req) returns http:Created|http:InternalServerError|error {
        string paymentId = uuid:createType1AsString();
        string currentTime = time:utcToString(time:utcNow());
        
        Payment payment = {
            id: paymentId,
            orderId: req.orderId,
            customerId: req.customerId,
            amount: req.amount,
            method: req.method,
            status: PENDING,
            createdAt: currentTime,
            updatedAt: currentTime
        };
        
        payments[paymentId] = payment;
        
        Payment[] orderPayments = paymentsByOrderId[req.orderId] ?: [];
        orderPayments.push(payment);
        paymentsByOrderId[req.orderId] = orderPayments;

        _ = @strand {thread: "any"} start processPaymentAsync(paymentId);

        return <http:Created>{ body: payment };
    }

    resource function get payments(string? orderId, string? customerId, string? status) returns Payment[] {
        Payment[] result = [];
        foreach var p in payments {
            if (orderId != () && p.orderId != orderId) {
                continue;
            }
            if (customerId != () && p.customerId != customerId) {
                continue;
            }
            if (status != () && p.status.toString() != status) {
                continue;
            }
            result.push(p);
        }
        return result;
    }

    resource function get payments/[string id]() returns Payment|http:NotFound {
        if payments.hasKey(id) {
            return payments.get(id);
        }
        return <http:NotFound>{ body: { message: "Payment not found" } };
    }

    resource function post payments/[string id]/refund(RefundRequest req) returns Payment|http:NotFound|http:BadRequest|error {
        if !payments.hasKey(id) {
            return <http:NotFound>{ body: { message: "Payment not found" } };
        }
        
        Payment payment = payments.get(id);
        if payment.status != COMPLETED {
            return <http:BadRequest>{ body: { message: "Only completed payments can be refunded" } };
        }
        
        payment.status = REFUNDED;
        payment.updatedAt = time:utcToString(time:utcNow());
        payments[id] = payment;

        PaymentEvent ev = { paymentId: payment.id, orderId: payment.orderId, amount: payment.amount };
        check publishEvent("payments.refunded", ev);

        log:printInfo("Payment refunded", paymentId = payment.id, reason = req.reason);
        return payment;
    }

    resource function get payments/'order/[string orderId]() returns Payment[] {
        return paymentsByOrderId[orderId] ?: [];
    }
}

function processPaymentAsync(string paymentId) returns error? {
    if !payments.hasKey(paymentId) {
        return;
    }

    Payment payment = payments.get(paymentId);
    payment.status = PROCESSING;
    payment.updatedAt = time:utcToString(time:utcNow());
    payments[paymentId] = payment;

    log:printInfo("Processing payment", paymentId = paymentId);

    runtime:sleep(2);

    float rand = random:createDecimal();

    payment.processedAt = time:utcToString(time:utcNow());
    payment.updatedAt = time:utcToString(time:utcNow());

    if rand <= 0.90 {
        payment.status = COMPLETED;
        payment.transactionRef = "TXN-" + uuid:createType1AsString().substring(0, 8);
        payments[paymentId] = payment;
        
        PaymentEvent ev = { paymentId: payment.id, orderId: payment.orderId, amount: payment.amount };
        check publishEvent("payments.completed", ev);
        log:printInfo("Payment successful", paymentId = payment.id, transactionRef = payment.transactionRef);
    } else {
        payment.status = FAILED;
        payments[paymentId] = payment;
        
        PaymentEvent ev = { paymentId: payment.id, orderId: payment.orderId, amount: payment.amount };
        check publishEvent("payments.failed", ev);
        log:printError("Payment failed", paymentId = payment.id);
    }
}

function publishEvent(string topic, anydata payload) returns error? {
    byte[] serializedPayload = payload.toJsonString().toBytes();
    check kafkaProducer->send({
        topic: topic,
        value: serializedPayload
    });
}

service on new kafka:Listener(kafkaBootstrapServers, {
    groupId: "payment-service-group",
    topics: ["orders.created", "orders.cancelled"]
}) {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payload = check string:fromBytes(rec.value);
            json jsonPayload = check payload.fromJsonString();
            string topic = rec.offset.partition.topic;
            
            if topic == "orders.created" {
                OrderEvent|error orderEvent = jsonPayload.cloneWithType(OrderEvent);
                if orderEvent is OrderEvent {
                    string paymentId = uuid:createType1AsString();
                    string currentTime = time:utcToString(time:utcNow());
                    
                    Payment payment = {
                        id: paymentId,
                        orderId: orderEvent.orderId,
                        customerId: orderEvent.customerId,
                        amount: orderEvent.totalAmount,
                        method: CREDIT_CARD,
                        status: PENDING,
                        createdAt: currentTime,
                        updatedAt: currentTime
                    };
                    payments[paymentId] = payment;
                    Payment[] orderPayments = paymentsByOrderId[orderEvent.orderId] ?: [];
                    orderPayments.push(payment);
                    paymentsByOrderId[orderEvent.orderId] = orderPayments;
                    
                    log:printInfo("Auto-initiated payment for new order", orderId = orderEvent.orderId);
                    _ = @strand {thread: "any"} start processPaymentAsync(paymentId);
                }
                
            } else if topic == "orders.cancelled" {
                json|error orderIdVal = jsonPayload.orderId;
                if orderIdVal is json && orderIdVal !is () {
                    string orderId = orderIdVal.toString();
                    Payment[] existingPayments = paymentsByOrderId[orderId] ?: [];
                    foreach var p in existingPayments {
                        if p.status == COMPLETED {
                            p.status = REFUNDED;
                            p.updatedAt = time:utcToString(time:utcNow());
                            payments[p.id] = p;
                            
                            PaymentEvent ev = { paymentId: p.id, orderId: p.orderId, amount: p.amount };
                            check publishEvent("payments.refunded", ev);
                            log:printInfo("Auto-refunded payment for cancelled order", orderId = orderId);
                        }
                    }
                }
            }
        }
    }
}
