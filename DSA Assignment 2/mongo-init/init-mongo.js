// MongoDB initialization script
// This script creates the required databases and collections for each microservice

// Customer Service Database
db = db.getSiblingDB('customer_db');
db.createCollection('customers');
db.customers.createIndex({ "email": 1 }, { unique: true });
db.customers.createIndex({ "phone": 1 });
db.createCollection('addresses');
db.addresses.createIndex({ "customerId": 1 });
print('✓ customer_db initialized');

// Restaurant Service Database  
db = db.getSiblingDB('restaurant_db');
db.createCollection('restaurants');
db.restaurants.createIndex({ "name": 1 });
db.restaurants.createIndex({ "cuisine": 1 });
db.restaurants.createIndex({ "isOpen": 1 });
db.createCollection('menu_items');
db.menu_items.createIndex({ "restaurantId": 1 });
db.menu_items.createIndex({ "category": 1 });
print('✓ restaurant_db initialized');

// Order Service Database
db = db.getSiblingDB('order_db');
db.createCollection('orders');
db.orders.createIndex({ "customerId": 1 });
db.orders.createIndex({ "restaurantId": 1 });
db.orders.createIndex({ "status": 1 });
db.orders.createIndex({ "createdAt": -1 });
db.createCollection('order_events');
db.order_events.createIndex({ "orderId": 1 });
print('✓ order_db initialized');

// Payment Service Database
db = db.getSiblingDB('payment_db');
db.createCollection('payments');
db.payments.createIndex({ "orderId": 1 }, { unique: true });
db.payments.createIndex({ "customerId": 1 });
db.payments.createIndex({ "status": 1 });
db.payments.createIndex({ "transactionRef": 1 });
print('✓ payment_db initialized');

// Delivery Service Database
db = db.getSiblingDB('delivery_db');
db.createCollection('drivers');
db.drivers.createIndex({ "email": 1 }, { unique: true });
db.drivers.createIndex({ "status": 1 });
db.createCollection('deliveries');
db.deliveries.createIndex({ "orderId": 1 });
db.deliveries.createIndex({ "driverId": 1 });
db.deliveries.createIndex({ "status": 1 });
print('✓ delivery_db initialized');

print('');
print('============================================');
print(' All databases initialized successfully!');
print('============================================');
