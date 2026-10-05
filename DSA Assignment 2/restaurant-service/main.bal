import ballerina/http;
import ballerinax/kafka;
import ballerina/uuid;
import ballerina/time;
import ballerina/log;

configurable string dbHost = "mongodb";
configurable int dbPort = 27017;
configurable string dbName = "restaurant_db";
configurable string kafkaBootstrapServers = "kafka:9092";
configurable int servicePort = 8082;

public type TimeSlot record {|
    string open;
    string close;
|};

public type OperatingHours record {|
    TimeSlot monday?;
    TimeSlot tuesday?;
    TimeSlot wednesday?;
    TimeSlot thursday?;
    TimeSlot friday?;
    TimeSlot saturday?;
    TimeSlot sunday?;
|};

public type MenuItem record {|
    string id;
    string name;
    string description;
    decimal price;
    string category;
    string imageUrl;
    boolean isAvailable;
    int preparationTime;
|};

public type Restaurant record {|
    string id;
    string name;
    string description;
    string address;
    string phone;
    string email;
    string[] cuisine;
    float rating;
    boolean isOpen;
    OperatingHours openingHours;
    MenuItem[] menu;
    string createdAt;
    string updatedAt;
|};

public type RestaurantInput record {|
    string name;
    string description;
    string address;
    string phone;
    string email;
    string[] cuisine;
    OperatingHours openingHours;
|};

public type MenuItemInput record {|
    string name;
    string description;
    decimal price;
    string category;
    string imageUrl;
    boolean isAvailable;
    int preparationTime;
|};

isolated map<Restaurant> restaurantsTable = {};

kafka:Producer kafkaProducer = check new (kafkaBootstrapServers);

isolated function publishEvent(string topic, string eventType, anydata payload) returns error? {
    map<anydata> event = {
        "eventId": uuid:createType1AsString(),
        "eventType": eventType,
        "timestamp": time:utcToString(time:utcNow()),
        "payload": payload
    };
    byte[] serializedEvent = event.toJsonString().toBytes();
    check kafkaProducer->send({
        topic: topic,
        value: serializedEvent
    });
    log:printInfo(string `Event ${eventType} published to topic ${topic}`);
}

listener kafka:Listener orderEventListener = new (kafkaBootstrapServers, {
    groupId: "restaurant-service-group",
    topics: "orders.created"
});

service on orderEventListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string payload = check string:fromBytes(rec.value);
            json jsonPayload = check payload.fromJsonString();
            
            string|error restaurantIdVal = jsonPayload.restaurantId;
            string|error orderIdVal = jsonPayload.id;
            
            if restaurantIdVal is string && orderIdVal is string {
                Restaurant? restaurant;
                lock {
                    restaurant = restaurantsTable[restaurantIdVal];
                }
                
                if restaurant is Restaurant {
                    log:printInfo("Received order for restaurant", orderId = orderIdVal, restaurantId = restaurantIdVal);
                    
                    boolean allItemsAvailable = true;
                    if jsonPayload.items is json[] {
                        foreach var item in <json[]>jsonPayload.items {
                            string|error menuItemId = item.menuItemId;
                            if menuItemId is string {
                                boolean itemFound = false;
                                foreach var menuItem in restaurant.menu {
                                    if menuItem.id == menuItemId && menuItem.isAvailable {
                                        itemFound = true;
                                        break;
                                    }
                                }
                                if !itemFound {
                                    allItemsAvailable = false;
                                    log:printError("Menu item not available", menuItemId = menuItemId);
                                }
                            }
                        }
                    }
                    
                    if allItemsAvailable {
                        log:printInfo("All menu items available for order", orderId = orderIdVal);
                    } else {
                        log:printError("Some menu items not available for order", orderId = orderIdVal);
                    }
                } else {
                    log:printError("Restaurant not found for order", restaurantId = restaurantIdVal);
                }
            }
        }
    }
}


@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"],
        allowCredentials: true,
        allowHeaders: ["*"],
        exposeHeaders: ["*"],
        maxAge: 84900
    }
}
service /restaurants on new http:Listener(servicePort) {

    resource function post .(@http:Payload RestaurantInput input) returns http:Created|http:BadRequest {
        string newId = uuid:createType1AsString();
        string currentTime = time:utcToString(time:utcNow());

        Restaurant newRestaurant = {
            id: newId,
            name: input.name,
            description: input.description,
            address: input.address,
            phone: input.phone,
            email: input.email,
            cuisine: input.cuisine,
            rating: 0.0,
            isOpen: false,
            openingHours: input.openingHours,
            menu: [],
            createdAt: currentTime,
            updatedAt: currentTime
        };

        lock {
            restaurantsTable[newId] = newRestaurant.clone();
        }

        error? publishResult = publishEvent("restaurant.events", "RestaurantRegistered", newRestaurant);
        if publishResult is error {
            log:printError("Failed to publish RestaurantRegistered event", 'error = publishResult);
        }

        return <http:Created>{body: newRestaurant};
    }

    resource function get . (string? cuisine, boolean? isOpen) returns Restaurant[] {
        Restaurant[] resultList = [];
        lock {
            foreach Restaurant r in restaurantsTable {
                boolean matchCuisine = true;
                if cuisine is string {
                    matchCuisine = r.cuisine.indexOf(cuisine) != ();
                }
                boolean matchStatus = true;
                if isOpen is boolean {
                    matchStatus = (r.isOpen == isOpen);
                }

                if matchCuisine && matchStatus {
                    resultList.push(r.clone());
                }
            }
        }
        return resultList;
    }

    resource function get [string id]() returns Restaurant|http:NotFound {
        Restaurant? res;
        lock {
            res = restaurantsTable[id];
        }
        if res is Restaurant {
            return res.clone();
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }

    resource function put [string id](@http:Payload RestaurantInput input) returns Restaurant|http:NotFound|http:InternalServerError {
        Restaurant? existing;
        lock {
            existing = restaurantsTable[id];
        }

        if existing is Restaurant {
            Restaurant updated = existing.clone();
            updated.name = input.name;
            updated.description = input.description;
            updated.address = input.address;
            updated.phone = input.phone;
            updated.email = input.email;
            updated.cuisine = input.cuisine;
            updated.openingHours = input.openingHours;
            updated.updatedAt = time:utcToString(time:utcNow());
            
            lock {
                restaurantsTable[id] = updated.clone();
            }

            error? publishResult = publishEvent("restaurant.events", "RestaurantUpdated", updated);
            if publishResult is error {
                 log:printError("Failed to publish RestaurantUpdated event", 'error = publishResult);
            }

            return updated;
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }

    resource function delete [string id]() returns http:NoContent|http:NotFound {
        Restaurant? existing;
        lock {
            existing = restaurantsTable.remove(id);
        }
        if existing is Restaurant {
             error? publishResult = publishEvent("restaurant.events", "RestaurantDeleted", { id: id });
             if publishResult is error {
                  log:printError("Failed to publish RestaurantDeleted event", 'error = publishResult);
             }
            return <http:NoContent>{};
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }

    resource function post [string id]/menu(@http:Payload MenuItemInput input) returns http:Created|http:NotFound {
        Restaurant? res;
        lock {
            res = restaurantsTable[id];
        }

        if res is Restaurant {
            MenuItem newItem = {
                id: uuid:createType1AsString(),
                name: input.name,
                description: input.description,
                price: input.price,
                category: input.category,
                imageUrl: input.imageUrl,
                isAvailable: input.isAvailable,
                preparationTime: input.preparationTime
            };

            Restaurant updated = res.clone();
            updated.menu.push(newItem);
            updated.updatedAt = time:utcToString(time:utcNow());

            lock {
                restaurantsTable[id] = updated.clone();
            }

            error? publishResult = publishEvent("restaurant.events", "MenuItemAdded", { restaurantId: id, item: newItem });
            if publishResult is error {
                 log:printError("Failed to publish MenuItemAdded event", 'error = publishResult);
            }

            return <http:Created>{body: newItem};
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }

    resource function get [string id]/menu() returns MenuItem[]|http:NotFound {
        Restaurant? res;
        lock {
            res = restaurantsTable[id];
        }
        if res is Restaurant {
            return res.menu.clone();
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }

    resource function put [string id]/menu/[string itemId](@http:Payload MenuItemInput input) returns MenuItem|http:NotFound {
        Restaurant? res;
        lock {
            res = restaurantsTable[id];
        }

        if res is Restaurant {
            Restaurant updated = res.clone();
            int? itemIndex = ();
            foreach int i in 0 ..< updated.menu.length() {
                if updated.menu[i].id == itemId {
                    itemIndex = i;
                    break;
                }
            }

            if itemIndex is int {
                MenuItem updatedItem = {
                    id: itemId,
                    name: input.name,
                    description: input.description,
                    price: input.price,
                    category: input.category,
                    imageUrl: input.imageUrl,
                    isAvailable: input.isAvailable,
                    preparationTime: input.preparationTime
                };
                
                updated.menu[itemIndex] = updatedItem;
                updated.updatedAt = time:utcToString(time:utcNow());

                lock {
                    restaurantsTable[id] = updated.clone();
                }

                error? publishResult = publishEvent("restaurant.events", "MenuItemUpdated", { restaurantId: id, item: updatedItem });
                if publishResult is error {
                     log:printError("Failed to publish MenuItemUpdated event", 'error = publishResult);
                }

                return updatedItem;
            }
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }

    resource function delete [string id]/menu/[string itemId]() returns http:NoContent|http:NotFound {
        Restaurant? res;
        lock {
            res = restaurantsTable[id];
        }

        if res is Restaurant {
            Restaurant updated = res.clone();
            int? itemIndex = ();
            foreach int i in 0 ..< updated.menu.length() {
                if updated.menu[i].id == itemId {
                    itemIndex = i;
                    break;
                }
            }

            if itemIndex is int {
                MenuItem _ = updated.menu.remove(itemIndex);
                updated.updatedAt = time:utcToString(time:utcNow());

                lock {
                    restaurantsTable[id] = updated.clone();
                }

                error? publishResult = publishEvent("restaurant.events", "MenuItemRemoved", { restaurantId: id, itemId: itemId });
                if publishResult is error {
                     log:printError("Failed to publish MenuItemRemoved event", 'error = publishResult);
                }

                return <http:NoContent>{};
            }
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }

    resource function put [string id]/status(@http:Payload record {| boolean isOpen; |} statusInput) returns Restaurant|http:NotFound {
        Restaurant? res;
        lock {
            res = restaurantsTable[id];
        }

        if res is Restaurant {
            Restaurant updated = res.clone();
            updated.isOpen = statusInput.isOpen;
            updated.updatedAt = time:utcToString(time:utcNow());

            lock {
                restaurantsTable[id] = updated.clone();
            }

            error? publishResult = publishEvent("restaurant.events", "RestaurantStatusChanged", { restaurantId: id, isOpen: statusInput.isOpen });
            if publishResult is error {
                 log:printError("Failed to publish RestaurantStatusChanged event", 'error = publishResult);
            }

            return updated;
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }

    resource function get [string id]/availability() returns record {| boolean isOpen; |}|http:NotFound {
        Restaurant? res;
        lock {
            res = restaurantsTable[id];
        }

        if res is Restaurant {
            return { isOpen: res.isOpen };
        }
        return <http:NotFound>{body: {message: "Restaurant not found"}};
    }
}

