import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerina/time;
import ballerinax/kafka;

configurable string dbHost = ?;
configurable int dbPort = ?;
configurable string dbName = ?;
configurable string kafkaBootstrapServers = ?;
configurable int servicePort = ?;

public type Address record {|
    string id;
    string label;
    string street;
    string city;
    string state;
    string zipCode;
    float latitude?;
    float longitude?;
    boolean isDefault;
|};

public type AddressInput record {|
    string label;
    string street;
    string city;
    string state;
    string zipCode;
    float latitude?;
    float longitude?;
    boolean isDefault;
|};

public type Customer record {|
    readonly string id;
    string name;
    string email;
    string phone;
    Address[] addresses;
    string createdAt;
    string updatedAt;
|};

public type CustomerInput record {|
    string name;
    string email;
    string phone;
|};

public type OrderEvent record {|
    string orderId;
    string customerId;
    string status;
    string createdAt;
    float totalAmount;
|};

isolated map<Customer> customersDb = {};
isolated map<OrderEvent[]> customerOrdersDb = {};

kafka:Producer customerEventProducer = check new (kafkaBootstrapServers);

listener kafka:Listener orderEventListener = new (kafkaBootstrapServers, {
    groupId: "customer-service-group",
    topics: "orders.created"
});

service kafka:Service on orderEventListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:AnydataConsumerRecord[] records) returns error? {
        foreach var rec in records {
            byte[] value = check trap <byte[]>rec.value;
            string message = check string:fromBytes(value);
            json jsonPayload = check message.fromJsonString();
            
            string|error customerIdVal = jsonPayload.customerId;
            string|error orderIdVal = jsonPayload.id;
            string|error statusVal = jsonPayload.status;
            string|error createdAtVal = jsonPayload.createdAt;
            decimal|error totalAmountVal = jsonPayload.totalAmount;
            
            if customerIdVal is string && orderIdVal is string && statusVal is string && createdAtVal is string && totalAmountVal is decimal {
                OrderEvent orderEvent = {
                    orderId: orderIdVal,
                    customerId: customerIdVal,
                    status: statusVal,
                    createdAt: createdAtVal,
                    totalAmount: <float>totalAmountVal
                };
                
                lock {
                    if customerOrdersDb.hasKey(customerIdVal) {
                        OrderEvent[] existingOrders = customerOrdersDb.get(customerIdVal);
                        existingOrders.push(orderEvent);
                        customerOrdersDb[customerIdVal] = existingOrders.clone();
                    } else {
                        customerOrdersDb[customerIdVal] = [orderEvent];
                    }
                }
                
                log:printInfo("Order event recorded for customer", customerId = customerIdVal, orderId = orderIdVal);
            }
        }
    }
}

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service / on new http:Listener(servicePort) {

    resource function post customers(CustomerInput input) returns http:Created|error {
        string newId = uuid:createType1AsString();
        string currentTime = time:utcToString(time:utcNow());
        
        Customer newCustomer = {
            id: newId,
            name: input.name,
            email: input.email,
            phone: input.phone,
            addresses: [],
            createdAt: currentTime,
            updatedAt: currentTime
        };
        
        lock {
            customersDb[newId] = newCustomer.clone();
        }
        
        check customerEventProducer->send({
            topic: "customer.events",
            value: newId.toBytes()
        });
        
        log:printInfo("Customer created: " + newId);
        return <http:Created>{ body: newCustomer };
    }

    resource function get customers() returns Customer[] {
        lock {
            return customersDb.toArray().clone();
        }
    }

    resource function get customers/[string id]() returns Customer|http:NotFound {
        Customer? customer;
        lock {
            customer = customersDb[id].clone();
        }
        if customer is Customer {
            return customer;
        }
        return <http:NotFound>{};
    }

    resource function put customers/[string id](CustomerInput input) returns Customer|http:NotFound|error {
        Customer? existing;
        lock {
            existing = customersDb[id].clone();
        }
        
        if existing is Customer {
            string currentTime = time:utcToString(time:utcNow());
            Customer updated = {
                id: id,
                name: input.name,
                email: input.email,
                phone: input.phone,
                addresses: existing.addresses,
                createdAt: existing.createdAt,
                updatedAt: currentTime
            };
            
            lock {
                customersDb[id] = updated.clone();
            }
            
            check customerEventProducer->send({
                topic: "customer.events",
                value: id.toBytes()
            });
            
            log:printInfo("Customer updated: " + id);
            return updated;
        }
        return <http:NotFound>{};
    }

    resource function delete customers/[string id]() returns http:NoContent|http:NotFound|error {
        boolean exists = false;
        lock {
            if customersDb.hasKey(id) {
                exists = true;
                _ = customersDb.remove(id);
            }
        }
        
        if exists {
            check customerEventProducer->send({
                topic: "customer.events",
                value: id.toBytes()
            });
            log:printInfo("Customer deleted: " + id);
            return <http:NoContent>{};
        }
        return <http:NotFound>{};
    }

    resource function post customers/[string id]/addresses(AddressInput input) returns http:Created|http:NotFound {
        Customer? customer;
        lock {
            customer = customersDb[id].clone();
        }
        
        if customer is Customer {
            Address newAddress = {
                id: uuid:createType1AsString(),
                label: input.label,
                street: input.street,
                city: input.city,
                state: input.state,
                zipCode: input.zipCode,
                latitude: input.latitude,
                longitude: input.longitude,
                isDefault: input.isDefault
            };
            
            customer.addresses.push(newAddress);
            customer.updatedAt = time:utcToString(time:utcNow());
            
            lock {
                customersDb[id] = customer.clone();
            }
            return <http:Created>{ body: newAddress };
        }
        return <http:NotFound>{};
    }

    resource function get customers/[string id]/addresses() returns Address[]|http:NotFound {
        Customer? customer;
        lock {
            customer = customersDb[id].clone();
        }
        if customer is Customer {
            return customer.addresses;
        }
        return <http:NotFound>{};
    }

    resource function put customers/[string id]/addresses/[string addressId](AddressInput input) returns Address|http:NotFound {
        Customer? customer;
        lock {
            customer = customersDb[id].clone();
        }
        
        if customer is Customer {
            int? addressIndex = ();
            foreach int i in 0 ..< customer.addresses.length() {
                if customer.addresses[i].id == addressId {
                    addressIndex = i;
                    break;
                }
            }
            
            if addressIndex is int {
                Address updatedAddress = {
                    id: addressId,
                    label: input.label,
                    street: input.street,
                    city: input.city,
                    state: input.state,
                    zipCode: input.zipCode,
                    latitude: input.latitude,
                    longitude: input.longitude,
                    isDefault: input.isDefault
                };
                
                customer.addresses[addressIndex] = updatedAddress;
                customer.updatedAt = time:utcToString(time:utcNow());
                
                lock {
                    customersDb[id] = customer.clone();
                }
                return updatedAddress;
            }
        }
        return <http:NotFound>{};
    }

    resource function delete customers/[string id]/addresses/[string addressId]() returns http:NoContent|http:NotFound {
        Customer? customer;
        lock {
            customer = customersDb[id].clone();
        }
        
        if customer is Customer {
            int? addressIndex = ();
            foreach int i in 0 ..< customer.addresses.length() {
                if customer.addresses[i].id == addressId {
                    addressIndex = i;
                    break;
                }
            }
            
            if addressIndex is int {
                _ = customer.addresses.remove(addressIndex);
                customer.updatedAt = time:utcToString(time:utcNow());
                
                lock {
                    customersDb[id] = customer.clone();
                }
                return <http:NoContent>{};
            }
        }
        return <http:NotFound>{};
    }

    resource function get customers/[string id]/orders() returns OrderEvent[]|http:NotFound {
        boolean customerExists = false;
        lock {
            customerExists = customersDb.hasKey(id);
        }
        
        if !customerExists {
            return <http:NotFound>{};
        }
        
        OrderEvent[] orders;
        lock {
            orders = customerOrdersDb.hasKey(id) ? customerOrdersDb.get(id).clone() : [];
        }
        
        return orders;
    }
}
