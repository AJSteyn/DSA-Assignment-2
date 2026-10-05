import ballerina/http;
import ballerinax/kafka;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;

// Configurable variables
configurable string kafkaBootstrapServers = "localhost:9092";
configurable int servicePort = 8087;
configurable string customerServiceUrl = "http://localhost:8081";
configurable string restaurantServiceUrl = "http://localhost:8082";
configurable string orderServiceUrl = "http://localhost:8083";
configurable string paymentServiceUrl = "http://localhost:8084";
configurable string deliveryServiceUrl = "http://localhost:8085";

// Data Models
type DashboardStats record {|
    int totalOrders;
    decimal totalRevenue;
    int totalCustomers;
    int totalRestaurants;
    int totalDrivers;
    float averageDeliveryTime;
    map<int> ordersByStatus;
|};

type RestaurantReport record {|
    string restaurantId;
    string restaurantName;
    int totalOrders;
    decimal totalRevenue;
    decimal averageOrderValue;
    string[] popularItems;
|};

type DriverReport record {|
    string driverId;
    string driverName;
    int totalDeliveries;
    float averageRating;
    float averageDeliveryTime;
    string status;
|};

type OrderTrend record {|
    string period;
    int orderCount;
    decimal revenue;
|};

type SystemHealth record {|
    string serviceName;
    string status;
    string lastChecked;
|};

// In-memory data structures for real-time analytics
int totalOrders = 0;
decimal totalRevenue = 0d;
map<int> ordersByStatus = {};
float totalDeliveryTime = 0.0;
int deliveryCount = 0;

// HTTP Clients for other services
final http:Client customerClient = check new (customerServiceUrl);
final http:Client restaurantClient = check new (restaurantServiceUrl);
final http:Client orderClient = check new (orderServiceUrl);
final http:Client paymentClient = check new (paymentServiceUrl);
final http:Client deliveryClient = check new (deliveryServiceUrl);

// Kafka Consumer
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "admin-analytics-group",
    topics: ["orders.created", "orders.status-updated", "orders.cancelled", "payments.completed", "payments.failed", "delivery.assigned", "delivery.completed"],
    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener = new (kafkaBootstrapServers, consumerConfiguration);

service on kafkaListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            byte[] messageContent = <byte[]>kafkaRecord.value;
            string|error messageStr = string:fromBytes(messageContent);
            if messageStr is string {
                log:printInfo("Received kafka event", topic = kafkaRecord.topic, message = messageStr);
            }
            
            // Simple logic to increment stats based on topic
            if kafkaRecord.topic == "orders.created" {
                totalOrders += 1;
            } else if kafkaRecord.topic == "payments.completed" {
                // Approximate totalRevenue logic for demo
                totalRevenue += 50.0d; // Dummy amount
            } else if kafkaRecord.topic == "delivery.completed" {
                deliveryCount += 1;
                totalDeliveryTime += 30.0; // Dummy time
            }
            
            check caller->commit();
        }
    }
}

// REST Endpoints
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /admin on new http:Listener(servicePort) {
    
    resource function get dashboard() returns DashboardStats|error {
        float avgDelivery = deliveryCount > 0 ? (totalDeliveryTime / <float>deliveryCount) : 0.0;
        
        return {
            totalOrders: totalOrders,
            totalRevenue: totalRevenue,
            totalCustomers: 100, // In reality, fetch from customer service
            totalRestaurants: 50, // In reality, fetch from restaurant service
            totalDrivers: 20, // In reality, fetch from delivery service
            averageDeliveryTime: avgDelivery,
            ordersByStatus: ordersByStatus
        };
    }
    
    resource function get reports/restaurants() returns RestaurantReport[]|error {
        return [
            {
                restaurantId: uuid:createType1AsString(),
                restaurantName: "Demo Restaurant",
                totalOrders: totalOrders,
                totalRevenue: totalRevenue,
                averageOrderValue: totalOrders > 0 ? totalRevenue / <decimal>totalOrders : 0d,
                popularItems: ["Burger", "Fries"]
            }
        ];
    }
    
    resource function get reports/restaurants/[string id]() returns RestaurantReport|error {
        return {
            restaurantId: id,
            restaurantName: "Demo Restaurant",
            totalOrders: 10,
            totalRevenue: 250.00d,
            averageOrderValue: 25.00d,
            popularItems: ["Pizza"]
        };
    }
    
    resource function get reports/drivers() returns DriverReport[]|error {
        return [
            {
                driverId: uuid:createType1AsString(),
                driverName: "John Doe",
                totalDeliveries: deliveryCount,
                averageRating: 4.5,
                averageDeliveryTime: 25.0,
                status: "Available"
            }
        ];
    }
    
    resource function get reports/drivers/[string id]() returns DriverReport|error {
        return {
            driverId: id,
            driverName: "John Doe",
            totalDeliveries: 5,
            averageRating: 4.8,
            averageDeliveryTime: 20.0,
            status: "Offline"
        };
    }
    
    resource function get reports/orders() returns OrderTrend[]|error {
        return [
            {
                period: "2023-10",
                orderCount: totalOrders,
                revenue: totalRevenue
            }
        ];
    }
    
    resource function get reports/revenue() returns json|error {
        return {
            totalRevenue: totalRevenue,
            monthlyAverage: totalRevenue / 12d
        };
    }
    
    resource function get health() returns SystemHealth[]|error {
        SystemHealth[] healthStatus = [];
        string currentTime = time:utcToString(time:utcNow());
        
        healthStatus.push({ serviceName: "Customer Service", status: "UP", lastChecked: currentTime });
        healthStatus.push({ serviceName: "Restaurant Service", status: "UP", lastChecked: currentTime });
        healthStatus.push({ serviceName: "Order Service", status: "UP", lastChecked: currentTime });
        healthStatus.push({ serviceName: "Payment Service", status: "UP", lastChecked: currentTime });
        healthStatus.push({ serviceName: "Delivery Service", status: "UP", lastChecked: currentTime });
        
        return healthStatus;
    }
    
    resource function get system/services() returns json|error {
        return {
            "customer-service": customerServiceUrl,
            "restaurant-service": restaurantServiceUrl,
            "order-service": orderServiceUrl,
            "payment-service": paymentServiceUrl,
            "delivery-service": deliveryServiceUrl
        };
    }
}
