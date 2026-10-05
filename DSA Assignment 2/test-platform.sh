#!/bin/bash

# Distributed Food Delivery Platform - Test Script
# This script tests the complete order flow through all microservices

set -e

BASE_URL="http://localhost"
CUSTOMER_SERVICE="${BASE_URL}:8081"
RESTAURANT_SERVICE="${BASE_URL}:8082"
ORDER_SERVICE="${BASE_URL}:8083"
PAYMENT_SERVICE="${BASE_URL}:8084"
DELIVERY_SERVICE="${BASE_URL}:8085"
NOTIFICATION_SERVICE="${BASE_URL}:8086"
ADMIN_SERVICE="${BASE_URL}:8087"

echo "======================================"
echo "Testing Food Delivery Platform"
echo "======================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print test results
print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $2"
    else
        echo -e "${RED}✗${NC} $2"
    fi
}

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 5

# Test 1: Register a customer
echo "Test 1: Register a customer"
CUSTOMER_RESPONSE=$(curl -s -X POST "${CUSTOMER_SERVICE}/customers" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "phone": "+264811234567"
  }')
CUSTOMER_ID=$(echo $CUSTOMER_RESPONSE | jq -r '.id')
print_result $? "Customer registered with ID: $CUSTOMER_ID"
echo ""

# Test 2: Add delivery address
echo "Test 2: Add delivery address"
ADDRESS_RESPONSE=$(curl -s -X POST "${CUSTOMER_SERVICE}/customers/${CUSTOMER_ID}/addresses" \
  -H "Content-Type: application/json" \
  -d '{
    "label": "Home",
    "street": "456 Sam Nujoma Drive",
    "city": "Windhoek",
    "state": "Khomas",
    "zipCode": "9000",
    "isDefault": true
  }')
print_result $? "Delivery address added"
echo ""

# Test 3: Register a restaurant
echo "Test 3: Register a restaurant"
RESTAURANT_RESPONSE=$(curl -s -X POST "${RESTAURANT_SERVICE}/restaurants" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Pizza Palace",
    "description": "Best pizza in Windhoek",
    "cuisine": ["Italian", "Pizza"],
    "address": "123 Independence Ave, Windhoek",
    "phone": "+264812345678",
    "email": "info@pizzapalace.com",
    "openingHours": {
      "monday": {"open": "09:00", "close": "22:00"},
      "tuesday": {"open": "09:00", "close": "22:00"},
      "wednesday": {"open": "09:00", "close": "22:00"},
      "thursday": {"open": "09:00", "close": "22:00"},
      "friday": {"open": "09:00", "close": "23:00"},
      "saturday": {"open": "10:00", "close": "23:00"},
      "sunday": {"open": "10:00", "close": "21:00"}
    }
  }')
RESTAURANT_ID=$(echo $RESTAURANT_RESPONSE | jq -r '.id')
print_result $? "Restaurant registered with ID: $RESTAURANT_ID"
echo ""

# Test 4: Open restaurant
echo "Test 4: Open restaurant for business"
OPEN_RESPONSE=$(curl -s -X PUT "${RESTAURANT_SERVICE}/restaurants/${RESTAURANT_ID}/status" \
  -H "Content-Type: application/json" \
  -d '{
    "isOpen": true
  }')
print_result $? "Restaurant opened"
echo ""

# Test 5: Add menu items
echo "Test 5: Add menu items"
MENU_ITEM_1_RESPONSE=$(curl -s -X POST "${RESTAURANT_SERVICE}/restaurants/${RESTAURANT_ID}/menu" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Margherita Pizza",
    "description": "Classic tomato and mozzarella",
    "price": 95.00,
    "category": "Pizza",
    "imageUrl": "https://example.com/pizza.jpg",
    "isAvailable": true,
    "preparationTime": 20
  }')
MENU_ITEM_1_ID=$(echo $MENU_ITEM_1_RESPONSE | jq -r '.id')
print_result $? "Menu item 1 added with ID: $MENU_ITEM_1_ID"

MENU_ITEM_2_RESPONSE=$(curl -s -X POST "${RESTAURANT_SERVICE}/restaurants/${RESTAURANT_ID}/menu" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Pepperoni Pizza",
    "description": "Spicy pepperoni with cheese",
    "price": 110.00,
    "category": "Pizza",
    "imageUrl": "https://example.com/pepperoni.jpg",
    "isAvailable": true,
    "preparationTime": 25
  }')
MENU_ITEM_2_ID=$(echo $MENU_ITEM_2_RESPONSE | jq -r '.id')
print_result $? "Menu item 2 added with ID: $MENU_ITEM_2_ID"
echo ""

# Test 6: Register a driver
echo "Test 6: Register a driver"
DRIVER_RESPONSE=$(curl -s -X POST "${DELIVERY_SERVICE}/drivers" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Jane Smith",
    "email": "jane@example.com",
    "phone": "+264819876543",
    "vehicleType": "Motorcycle",
    "licensePlate": "N 1234"
  }')
DRIVER_ID=$(echo $DRIVER_RESPONSE | jq -r '.id')
print_result $? "Driver registered with ID: $DRIVER_ID"
echo ""

# Test 7: Set driver as available and update location
echo "Test 7: Set driver as available and update location"
STATUS_RESPONSE=$(curl -s -X PUT "${DELIVERY_SERVICE}/drivers/${DRIVER_ID}/status" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "AVAILABLE"
  }')
print_result $? "Driver set to AVAILABLE"

LOCATION_RESPONSE=$(curl -s -X PUT "${DELIVERY_SERVICE}/drivers/${DRIVER_ID}/location?latitude=-22.5597&longitude=17.0832")
print_result $? "Driver location updated"
echo ""

# Test 8: Place an order
echo "Test 8: Place an order"
ORDER_RESPONSE=$(curl -s -X POST "${ORDER_SERVICE}/orders" \
  -H "Content-Type: application/json" \
  -d "{
    \"customerId\": \"${CUSTOMER_ID}\",
    \"restaurantId\": \"${RESTAURANT_ID}\",
    \"items\": [
      {
        \"menuItemId\": \"${MENU_ITEM_1_ID}\",
        \"name\": \"Margherita Pizza\",
        \"quantity\": 2,
        \"unitPrice\": 95.00
      },
      {
        \"menuItemId\": \"${MENU_ITEM_2_ID}\",
        \"name\": \"Pepperoni Pizza\",
        \"quantity\": 1,
        \"unitPrice\": 110.00
      }
    ],
    \"deliveryAddress\": \"456 Sam Nujoma Drive, Windhoek\"
  }")
ORDER_ID=$(echo $ORDER_RESPONSE | jq -r '.id')
ORDER_STATUS=$(echo $ORDER_RESPONSE | jq -r '.status')
ORDER_TOTAL=$(echo $ORDER_RESPONSE | jq -r '.totalAmount')
print_result $? "Order placed with ID: $ORDER_ID, Status: $ORDER_STATUS, Total: NAD$ORDER_TOTAL"
echo ""

# Wait for payment processing
echo "Waiting for payment processing (5 seconds)..."
sleep 5

# Test 9: Check payment status
echo "Test 9: Check payment status"
PAYMENT_RESPONSE=$(curl -s "${PAYMENT_SERVICE}/payments/order/${ORDER_ID}")
PAYMENT_STATUS=$(echo $PAYMENT_RESPONSE | jq -r '.[0].status')
print_result $? "Payment status: $PAYMENT_STATUS"
echo ""

# Wait for order confirmation
echo "Waiting for order confirmation (5 seconds)..."
sleep 5

# Test 10: Check order status
echo "Test 10: Check order status"
ORDER_STATUS_RESPONSE=$(curl -s "${ORDER_SERVICE}/orders/${ORDER_ID}")
UPDATED_ORDER_STATUS=$(echo $ORDER_STATUS_RESPONSE | jq -r '.status')
print_result $? "Order status: $UPDATED_ORDER_STATUS"
echo ""

# Wait for delivery assignment
echo "Waiting for delivery assignment (5 seconds)..."
sleep 5

# Test 11: Check delivery status
echo "Test 11: Check delivery status"
DELIVERY_RESPONSE=$(curl -s "${DELIVERY_SERVICE}/deliveries/order/${ORDER_ID}")
DELIVERY_STATUS=$(echo $DELIVERY_RESPONSE | jq -r '.status')
ASSIGNED_DRIVER=$(echo $DELIVERY_RESPONSE | jq -r '.driverId')
print_result $? "Delivery status: $DELIVERY_STATUS, Assigned driver: $ASSIGNED_DRIVER"
echo ""

# Test 12: Update order status to PREPARING
echo "Test 12: Update order status to PREPARING"
PREPARING_RESPONSE=$(curl -s -X PUT "${ORDER_SERVICE}/orders/${ORDER_ID}/status" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "PREPARING",
    "changedBy": "restaurant-service",
    "reason": "Kitchen started preparation"
  }')
print_result $? "Order status updated to PREPARING"
echo ""

# Test 13: Update order status to READY
echo "Test 13: Update order status to READY"
READY_RESPONSE=$(curl -s -X PUT "${ORDER_SERVICE}/orders/${ORDER_ID}/status" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "READY",
    "changedBy": "restaurant-service",
    "reason": "Order is ready for pickup"
  }')
print_result $? "Order status updated to READY"
echo ""

# Test 14: Update order status to OUT_FOR_DELIVERY
echo "Test 14: Update order status to OUT_FOR_DELIVERY"
OUT_FOR_DELIVERY_RESPONSE=$(curl -s -X PUT "${ORDER_SERVICE}/orders/${ORDER_ID}/status" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "OUT_FOR_DELIVERY",
    "changedBy": "delivery-service",
    "reason": "Driver picked up order"
  }')
print_result $? "Order status updated to OUT_FOR_DELIVERY"
echo ""

# Test 15: Update delivery status to DELIVERED
echo "Test 15: Update delivery status to DELIVERED"
DELIVERED_RESPONSE=$(curl -s -X PUT "${DELIVERY_SERVICE}/deliveries/order/${ORDER_ID}/status" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "DELIVERED"
  }')
print_result $? "Delivery status updated to DELIVERED"
echo ""

# Wait for order status update
echo "Waiting for order status update (5 seconds)..."
sleep 5

# Test 16: Check final order status
echo "Test 16: Check final order status"
FINAL_ORDER_RESPONSE=$(curl -s "${ORDER_SERVICE}/orders/${ORDER_ID}")
FINAL_ORDER_STATUS=$(echo $FINAL_ORDER_RESPONSE | jq -r '.status')
print_result $? "Final order status: $FINAL_ORDER_STATUS"
echo ""

# Test 17: Track order history
echo "Test 17: Track order history"
TRACK_RESPONSE=$(curl -s "${ORDER_SERVICE}/orders/${ORDER_ID}/track")
print_result $? "Order tracking history retrieved"
echo ""

# Test 18: Check customer order history
echo "Test 18: Check customer order history"
CUSTOMER_ORDERS=$(curl -s "${CUSTOMER_SERVICE}/customers/${CUSTOMER_ID}/orders")
print_result $? "Customer order history retrieved"
echo ""

# Test 19: Check notifications
echo "Test 19: Check notifications"
NOTIFICATIONS=$(curl -s "${NOTIFICATION_SERVICE}/notifications/recipient/${CUSTOMER_ID}")
print_result $? "Notifications retrieved"
echo ""

# Test 20: Check admin dashboard
echo "Test 20: Check admin dashboard"
DASHBOARD=$(curl -s "${ADMIN_SERVICE}/admin/dashboard")
print_result $? "Admin dashboard statistics retrieved"
echo ""

# Test 21: Check system health
echo "Test 21: Check system health"
HEALTH=$(curl -s "${ADMIN_SERVICE}/admin/health")
print_result $? "System health check completed"
echo ""

echo "======================================"
echo "All tests completed!"
echo "======================================"
echo ""
echo "Summary:"
echo "- Customer ID: $CUSTOMER_ID"
echo "- Restaurant ID: $RESTAURANT_ID"
echo "- Order ID: $ORDER_ID"
echo "- Driver ID: $DRIVER_ID"
echo "- Final Order Status: $FINAL_ORDER_STATUS"
echo ""
