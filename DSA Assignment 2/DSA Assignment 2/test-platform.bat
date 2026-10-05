@echo off
REM Distributed Food Delivery Platform - Test Script (Windows)
REM This script tests the complete order flow through all microservices

setlocal enabledelayedexpansion

set BASE_URL=http://localhost
set CUSTOMER_SERVICE=%BASE_URL%:8081
set RESTAURANT_SERVICE=%BASE_URL%:8082
set ORDER_SERVICE=%BASE_URL%:8083
set PAYMENT_SERVICE=%BASE_URL%:8084
set DELIVERY_SERVICE=%BASE_URL%:8085
set NOTIFICATION_SERVICE=%BASE_URL%:8086
set ADMIN_SERVICE=%BASE_URL%:8087

echo ======================================
echo Testing Food Delivery Platform
echo ======================================
echo.

REM Wait for services to be ready
echo Waiting for services to be ready...
timeout /t 5 /nobreak >nul

REM Test 1: Register a customer
echo Test 1: Register a customer
curl -s -X POST "%CUSTOMER_SERVICE%/customers" -H "Content-Type: application/json" -d "{\"name\": \"John Doe\", \"email\": \"john@example.com\", \"phone\": \"+264811234567\"}" > customer_response.json
set /p CUSTOMER_ID=<customer_response.json
echo Customer response:
type customer_response.json
echo.
echo.

REM Test 2: Add delivery address
echo Test 2: Add delivery address
REM Note: You'll need to extract customer_id from the response above
curl -s -X POST "%CUSTOMER_SERVICE%/customers/CUSTOMER_ID/addresses" -H "Content-Type: application/json" -d "{\"label\": \"Home\", \"street\": \"456 Sam Nujoma Drive\", \"city\": \"Windhoek\", \"state\": \"Khomas\", \"zipCode\": \"9000\", \"isDefault\": true}" > address_response.json
echo Address response:
type address_response.json
echo.
echo.

REM Test 3: Register a restaurant
echo Test 3: Register a restaurant
curl -s -X POST "%RESTAURANT_SERVICE%/restaurants" -H "Content-Type: application/json" -d "{\"name\": \"Pizza Palace\", \"description\": \"Best pizza in Windhoek\", \"cuisine\": [\"Italian\", \"Pizza\"], \"address\": \"123 Independence Ave, Windhoek\", \"phone\": \"+264812345678\", \"email\": \"info@pizzapalace.com\", \"openingHours\": {\"monday\": {\"open\": \"09:00\", \"close\": \"22:00\"}, \"tuesday\": {\"open\": \"09:00\", \"close\": \"22:00\"}, \"wednesday\": {\"open\": \"09:00\", \"close\": \"22:00\"}, \"thursday\": {\"open\": \"09:00\", \"close\": \"22:00\"}, \"friday\": {\"open\": \"09:00\", \"close\": \"23:00\"}, \"saturday\": {\"open\": \"10:00\", \"close\": \"23:00\"}, \"sunday\": {\"open\": \"10:00\", \"close\": \"21:00\"}}}" > restaurant_response.json
echo Restaurant response:
type restaurant_response.json
echo.
echo.

REM Test 4: Open restaurant
echo Test 4: Open restaurant for business
curl -s -X PUT "%RESTAURANT_SERVICE%/restaurants/RESTAURANT_ID/status" -H "Content-Type: application/json" -d "{\"isOpen\": true}" > open_response.json
echo Open response:
type open_response.json
echo.
echo.

REM Test 5: Add menu items
echo Test 5: Add menu items
curl -s -X POST "%RESTAURANT_SERVICE%/restaurants/RESTAURANT_ID/menu" -H "Content-Type: application/json" -d "{\"name\": \"Margherita Pizza\", \"description\": \"Classic tomato and mozzarella\", \"price\": 95.00, \"category\": \"Pizza\", \"imageUrl\": \"https://example.com/pizza.jpg\", \"isAvailable\": true, \"preparationTime\": 20}" > menu1_response.json
echo Menu item 1 response:
type menu1_response.json
echo.
echo.

curl -s -X POST "%RESTAURANT_SERVICE%/restaurants/RESTAURANT_ID/menu" -H "Content-Type: application/json" -d "{\"name\": \"Pepperoni Pizza\", \"description\": \"Spicy pepperoni with cheese\", \"price\": 110.00, \"category\": \"Pizza\", \"imageUrl\": \"https://example.com/pepperoni.jpg\", \"isAvailable\": true, \"preparationTime\": 25}" > menu2_response.json
echo Menu item 2 response:
type menu2_response.json
echo.
echo.

REM Test 6: Register a driver
echo Test 6: Register a driver
curl -s -X POST "%DELIVERY_SERVICE%/drivers" -H "Content-Type: application/json" -d "{\"name\": \"Jane Smith\", \"email\": \"jane@example.com\", \"phone\": \"+264819876543\", \"vehicleType\": \"Motorcycle\", \"licensePlate\": \"N 1234\"}" > driver_response.json
echo Driver response:
type driver_response.json
echo.
echo.

REM Test 7: Set driver as available
echo Test 7: Set driver as available and update location
curl -s -X PUT "%DELIVERY_SERVICE%/drivers/DRIVER_ID/status" -H "Content-Type: application/json" -d "{\"status\": \"AVAILABLE\"}" > driver_status_response.json
echo Driver status response:
type driver_status_response.json
echo.
echo.

curl -s -X PUT "%DELIVERY_SERVICE%/drivers/DRIVER_ID/location?latitude=-22.5597&longitude=17.0832" > location_response.json
echo Location response:
type location_response.json
echo.
echo.

REM Test 8: Place an order
echo Test 8: Place an order
curl -s -X POST "%ORDER_SERVICE%/orders" -H "Content-Type: application/json" -d "{\"customerId\": \"CUSTOMER_ID\", \"restaurantId\": \"RESTAURANT_ID\", \"items\": [{\"menuItemId\": \"MENU_ITEM_1_ID\", \"name\": \"Margherita Pizza\", \"quantity\": 2, \"unitPrice\": 95.00}, {\"menuItemId\": \"MENU_ITEM_2_ID\", \"name\": \"Pepperoni Pizza\", \"quantity\": 1, \"unitPrice\": 110.00}], \"deliveryAddress\": \"456 Sam Nujoma Drive, Windhoek\"}" > order_response.json
echo Order response:
type order_response.json
echo.
echo.

REM Wait for payment processing
echo Waiting for payment processing (5 seconds)...
timeout /t 5 /nobreak >nul

REM Test 9: Check payment status
echo Test 9: Check payment status
curl -s "%PAYMENT_SERVICE%/payments/order/ORDER_ID" > payment_status.json
echo Payment status:
type payment_status.json
echo.
echo.

REM Wait for order confirmation
echo Waiting for order confirmation (5 seconds)...
timeout /t 5 /nobreak >nul

REM Test 10: Check order status
echo Test 10: Check order status
curl -s "%ORDER_SERVICE%/orders/ORDER_ID" > order_status.json
echo Order status:
type order_status.json
echo.
echo.

REM Wait for delivery assignment
echo Waiting for delivery assignment (5 seconds)...
timeout /t 5 /nobreak >nul

REM Test 11: Check delivery status
echo Test 11: Check delivery status
curl -s "%DELIVERY_SERVICE%/deliveries/order/ORDER_ID" > delivery_status.json
echo Delivery status:
type delivery_status.json
echo.
echo.

REM Test 12: Update order status to PREPARING
echo Test 12: Update order status to PREPARING
curl -s -X PUT "%ORDER_SERVICE%/orders/ORDER_ID/status" -H "Content-Type: application/json" -d "{\"status\": \"PREPARING\", \"changedBy\": \"restaurant-service\", \"reason\": \"Kitchen started preparation\"}" > preparing_response.json
echo Preparing response:
type preparing_response.json
echo.
echo.

REM Test 13: Update order status to READY
echo Test 13: Update order status to READY
curl -s -X PUT "%ORDER_SERVICE%/orders/ORDER_ID/status" -H "Content-Type: application/json" -d "{\"status\": \"READY\", \"changedBy\": \"restaurant-service\", \"reason\": \"Order is ready for pickup\"}" > ready_response.json
echo Ready response:
type ready_response.json
echo.
echo.

REM Test 14: Update order status to OUT_FOR_DELIVERY
echo Test 14: Update order status to OUT_FOR_DELIVERY
curl -s -X PUT "%ORDER_SERVICE%/orders/ORDER_ID/status" -H "Content-Type: application/json" -d "{\"status\": \"OUT_FOR_DELIVERY\", \"changedBy\": \"delivery-service\", \"reason\": \"Driver picked up order\"}" > out_for_delivery_response.json
echo Out for delivery response:
type out_for_delivery_response.json
echo.
echo.

REM Test 15: Update delivery status to DELIVERED
echo Test 15: Update delivery status to DELIVERED
curl -s -X PUT "%DELIVERY_SERVICE%/deliveries/order/ORDER_ID/status" -H "Content-Type: application/json" -d "{\"status\": \"DELIVERED\"}" > delivered_response.json
echo Delivered response:
type delivered_response.json
echo.
echo.

REM Wait for order status update
echo Waiting for order status update (5 seconds)...
timeout /t 5 /nobreak >nul

REM Test 16: Check final order status
echo Test 16: Check final order status
curl -s "%ORDER_SERVICE%/orders/ORDER_ID" > final_order_status.json
echo Final order status:
type final_order_status.json
echo.
echo.

REM Test 17: Track order history
echo Test 17: Track order history
curl -s "%ORDER_SERVICE%/orders/ORDER_ID/track" > track_response.json
echo Track response:
type track_response.json
echo.
echo.

REM Test 18: Check customer order history
echo Test 18: Check customer order history
curl -s "%CUSTOMER_SERVICE%/customers/CUSTOMER_ID/orders" > customer_orders.json
echo Customer orders:
type customer_orders.json
echo.
echo.

REM Test 19: Check notifications
echo Test 19: Check notifications
curl -s "%NOTIFICATION_SERVICE%/notifications/recipient/CUSTOMER_ID" > notifications.json
echo Notifications:
type notifications.json
echo.
echo.

REM Test 20: Check admin dashboard
echo Test 20: Check admin dashboard
curl -s "%ADMIN_SERVICE%/admin/dashboard" > dashboard.json
echo Dashboard:
type dashboard.json
echo.
echo.

REM Test 21: Check system health
echo Test 21: Check system health
curl -s "%ADMIN_SERVICE%/admin/health" > health.json
echo Health:
type health.json
echo.
echo.

echo ======================================
echo All tests completed!
echo ======================================
echo.
echo Note: This batch file saves responses to JSON files. You need to manually replace CUSTOMER_ID, RESTAURANT_ID, etc. with actual IDs from the responses.
echo For automated testing, use the bash script (test-platform.sh) with jq installed.
echo.

REM Cleanup
del customer_response.json address_response.json restaurant_response.json open_response.json menu1_response.json menu2_response.json driver_response.json driver_status_response.json location_response.json order_response.json payment_status.json order_status.json delivery_status.json preparing_response.json ready_response.json out_for_delivery_response.json delivered_response.json final_order_status.json track_response.json customer_orders.json notifications.json dashboard.json health.json

endlocal
