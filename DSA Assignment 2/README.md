# 🍔 Distributed Food Delivery Platform

## DSA612S - Distributed Systems and Applications | Assignment 2

A scalable, distributed Food Delivery Platform built with microservices architecture, event-driven communication via Apache Kafka, and containerised using Docker.

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [System Architecture Diagram](#system-architecture-diagram)
- [Technology Stack](#technology-stack)
- [Microservices](#microservices)
- [Kafka Topics & Event Flow](#kafka-topics--event-flow)
- [Database Schema Design](#database-schema-design)
- [Order Lifecycle State Machine](#order-lifecycle-state-machine)
- [Getting Started](#getting-started)
- [API Documentation](#api-documentation)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Implementation Status](#implementation-status)

---

## 🏗️ Architecture Overview

The platform follows a **microservices architecture** pattern where each service is independently deployable and communicates asynchronously through **Apache Kafka** event streaming. Services are containerised using **Docker** and orchestrated with **Docker Compose**.

### Key Architectural Principles

| Principle | Implementation |
|-----------|---------------|
| **Service Independence** | Each microservice has its own database and business logic |
| **Event-Driven Communication** | Asynchronous messaging via Kafka topics |
| **Fault Tolerance** | Services can operate independently; Kafka provides message durability |
| **Scalability** | Horizontal scaling through Kafka partitioning and Docker replicas |
| **Containerisation** | All services run in isolated Docker containers |

---

## 🔧 System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DISTRIBUTED FOOD DELIVERY PLATFORM                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │   Customer    │  │  Restaurant  │  │    Order     │  │   Payment    │   │
│  │   Service    │  │   Service    │  │   Service    │  │   Service    │   │
│  │   :8081      │  │   :8082      │  │   :8083      │  │   :8084      │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │
│         │                 │                 │                 │             │
│  ┌──────┴─────────────────┴─────────────────┴─────────────────┴──────┐     │
│  │                    APACHE KAFKA (Event Bus)                        │     │
│  │  Topics: orders.created | orders.confirmed | orders.cancelled     │     │
│  │          payments.completed | delivery.assigned | delivery.completed│    │
│  │          customer.events | restaurant.events | ...                 │     │
│  └──────┬─────────────────┬─────────────────┬────────────────────────┘     │
│         │                 │                 │                               │
│  ┌──────┴───────┐  ┌──────┴───────┐  ┌──────┴───────┐                     │
│  │   Delivery   │  │ Notification │  │    Admin     │                     │
│  │   Service    │  │   Service    │  │   Service    │                     │
│  │   :8085      │  │   :8086      │  │   :8087      │                     │
│  └──────────────┘  └──────────────┘  └──────────────┘                     │
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                     │
│  │   MongoDB    │  │  Zookeeper   │  │    Kafka     │                     │
│  │   :27017     │  │   :2181      │  │   :9092      │                     │
│  └──────────────┘  └──────────────┘  └──────────────┘                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Backend** | Ballerina 2201.12.0 | Microservice implementation |
| **Messaging** | Apache Kafka 7.5.0 | Event-driven async communication |
| **Database** | MongoDB 7.0 | Service-specific data persistence |
| **Coordination** | Apache Zookeeper | Kafka cluster management |
| **Containerisation** | Docker | Service isolation |
| **Orchestration** | Docker Compose | Multi-container management |

---

## 🔌 Microservices

### 1. Customer Service (Port 8081)
Manages user accounts, delivery addresses, and historical order data.

**Key Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/customers` | Register new customer |
| GET | `/customers` | List all customers |
| GET | `/customers/{id}` | Get customer by ID |
| PUT | `/customers/{id}` | Update customer profile |
| DELETE | `/customers/{id}` | Delete customer |
| POST | `/customers/{id}/addresses` | Add delivery address |
| GET | `/customers/{id}/orders` | Get order history |

### 2. Restaurant Service (Port 8082)
Manages digital menus, real-time inventory, and kitchen operating hours.

**Key Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/restaurants` | Register new restaurant |
| GET | `/restaurants` | List restaurants (filter by cuisine, status) |
| GET | `/restaurants/{id}` | Get restaurant details |
| POST | `/restaurants/{id}/menu` | Add menu item |
| GET | `/restaurants/{id}/menu` | Get full menu |
| PUT | `/restaurants/{id}/status` | Toggle open/closed |

### 3. Order Service (Port 8083) ⭐ Core Service
Manages the central order state machine and lifecycle.

**Key Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/orders` | Create new order |
| GET | `/orders` | List orders (filter by status, customer, restaurant) |
| GET | `/orders/{id}` | Get order details |
| PUT | `/orders/{id}/status` | Update order status |
| PUT | `/orders/{id}/cancel` | Cancel order |
| GET | `/orders/{id}/track` | Get order tracking history |

### 4. Payment Service (Port 8084)
Simulates payment processing and emits confirmation events.

**Key Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/payments` | Process payment |
| GET | `/payments/{id}` | Get payment details |
| POST | `/payments/{id}/refund` | Process refund |
| GET | `/payments/order/{orderId}` | Get payment for order |

### 5. Delivery Service (Port 8085)
Coordinates driver assignments and tracks real-time delivery status.

**Key Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/drivers` | Register new driver |
| GET | `/drivers` | List drivers (filter by status) |
| PUT | `/drivers/{id}/status` | Update driver availability |
| PUT | `/drivers/{id}/location` | Update driver location |
| GET | `/deliveries/{id}` | Get delivery details |
| POST | `/deliveries/{id}/assign` | Assign driver |

### 6. Notification Service (Port 8086)
Disseminates multi-channel alerts to customers, restaurants, and drivers.

**Key Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/notifications/recipient/{id}` | Get notifications |
| GET | `/notifications/recipient/{id}/unread` | Get unread count |
| PUT | `/notifications/{id}/read` | Mark as read |
| POST | `/notifications/send` | Send notification |

### 7. Admin Service (Port 8087)
Generates reports on restaurant statistics and delivery performance.

**Key Endpoints:**
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/admin/dashboard` | Platform statistics |
| GET | `/admin/reports/restaurants` | Restaurant performance |
| GET | `/admin/reports/drivers` | Driver performance |
| GET | `/admin/reports/orders` | Order analytics |
| GET | `/admin/reports/revenue` | Revenue reports |
| GET | `/admin/health` | System health check |

---

## 📨 Kafka Topics & Event Flow

### Topic Architecture

| Topic Name | Producer | Consumers | Purpose |
|------------|----------|-----------|---------|
| `orders.created` | Order Service | Payment, Restaurant, Customer, Notification, Admin | New order placed |
| `orders.confirmed` | Order Service | Delivery, Notification, Admin | Order confirmed after payment |
| `orders.status-updated` | Order Service | Notification, Admin | Any order status change |
| `orders.cancelled` | Order Service | Payment, Delivery, Notification, Admin | Order cancellation |
| `payments.completed` | Payment Service | Order, Notification, Admin | Payment successful |
| `payments.failed` | Payment Service | Notification, Admin | Payment failed |
| `payments.refunded` | Payment Service | Notification | Refund processed |
| `delivery.assigned` | Delivery Service | Order, Notification, Admin | Driver assigned |
| `delivery.picked-up` | Delivery Service | Notification | Order picked up |
| `delivery.completed` | Delivery Service | Order, Notification, Admin | Delivery complete |
| `delivery.failed` | Delivery Service | Notification | Delivery failed |
| `customer.events` | Customer Service | Admin | Customer registration/updates |
| `restaurant.events` | Restaurant Service | Admin | Restaurant updates |

### Event Flow Diagram

```
Customer places order
        │
        ▼
┌──────────────┐    orders.created    ┌──────────────┐
│ Order Service │──────────────────────▶│Payment Service│
│  (CREATED)    │──────────────────────▶│Restaurant Svc│
└──────────────┘──────────────────────▶│Customer Svc  │
        ▲                             └──────────────┘
        │                                     │
        │  payments.completed                 │
        ├─────────────────────────────────────┘
        │
        ▼
┌──────────────┐   orders.confirmed   ┌──────────────┐
│ Order Service │──────────────────────▶│Delivery Svc  │
│  (CONFIRMED)  │                      │(assigns driver)│
└──────────────┘                      └───────┬──────┘
        ▲                                     │
        │  delivery.assigned                  │
        ├─────────────────────────────────────┘
        │
        ▼
  [PREPARING → READY → OUT_FOR_DELIVERY]
        │
        ▼  delivery.completed
┌──────────────┐
│ Order Service │
│  (DELIVERED)  │
└──────────────┘
```

---

## 📊 Database Schema Design

Each microservice owns its data store following the **Database per Service** pattern.

### Customer DB
```
customers {
  _id: String (UUID)
  name: String
  email: String (unique)
  phone: String
  addresses: [Address]
  createdAt: DateTime
  updatedAt: DateTime
}
```

### Restaurant DB
```
restaurants {
  _id: String (UUID)
  name: String
  description: String
  cuisine: [String]
  rating: Float
  isOpen: Boolean
  openingHours: OperatingHours
  menu: [MenuItem]
}
```

### Order DB
```
orders {
  _id: String (UUID)
  customerId: String (FK)
  restaurantId: String (FK)
  items: [OrderItem]
  totalAmount: Decimal
  status: Enum (CREATED|CONFIRMED|PREPARING|READY|OUT_FOR_DELIVERY|DELIVERED|CANCELLED)
  statusHistory: [StatusChange]
  deliveryAddress: String
  driverId: String?
}
```

### Payment DB
```
payments {
  _id: String (UUID)
  orderId: String (FK)
  customerId: String (FK)
  amount: Decimal
  currency: String (default: "NAD")
  method: Enum (CREDIT_CARD|DEBIT_CARD|MOBILE_MONEY|CASH_ON_DELIVERY)
  status: Enum (PENDING|PROCESSING|COMPLETED|FAILED|REFUNDED)
  transactionRef: String
}
```

### Delivery DB
```
drivers {
  _id: String (UUID)
  name: String
  vehicleType: String
  status: Enum (AVAILABLE|BUSY|OFFLINE)
  currentLocation: {latitude, longitude}
  rating: Float
}

deliveries {
  _id: String (UUID)
  orderId: String (FK)
  driverId: String (FK)
  status: Enum (PENDING|ASSIGNED|PICKED_UP|IN_TRANSIT|DELIVERED|FAILED)
  estimatedTime: Int (minutes)
}
```

---

## 🔄 Order Lifecycle State Machine

```
                    ┌────────────────────────────────────────┐
                    │             CANCELLED                   │
                    │  (can be reached from any non-terminal) │
                    └────────────────────────────────────────┘
                           ▲    ▲    ▲    ▲    ▲
                           │    │    │    │    │
┌─────────┐  payment  ┌───┴────┴┐  ┌┴────┴─┐  ┌┴────────────────┐  ┌───────────┐
│ CREATED  │─────────▶│CONFIRMED│─▶│PREPARING│─▶│     READY       │─▶│OUT_FOR_   │
│          │ completed│         │  │        │  │                 │  │ DELIVERY  │
└─────────┘          └─────────┘  └────────┘  └─────────────────┘  └─────┬─────┘
                                                                         │
                                                                         ▼
                                                                   ┌───────────┐
                                                                   │ DELIVERED  │
                                                                   │ (terminal) │
                                                                   └───────────┘
```

**Valid Transitions:**
| From | To (Valid) |
|------|-----------|
| CREATED | CONFIRMED, CANCELLED |
| CONFIRMED | PREPARING, CANCELLED |
| PREPARING | READY, CANCELLED |
| READY | OUT_FOR_DELIVERY, CANCELLED |
| OUT_FOR_DELIVERY | DELIVERED, CANCELLED |
| DELIVERED | _(terminal state)_ |
| CANCELLED | _(terminal state)_ |

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (version 20.10+)
- [Docker Compose](https://docs.docker.com/compose/install/) (version 2.0+)
- [Ballerina](https://ballerina.io/downloads/) (Swan Lake 2201.12.0) - for local development

### Quick Start

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd distributed-food-delivery-platform
   ```

2. **Start all services:**
   ```bash
   docker-compose up -d --build
   ```

3. **Verify services are running:**
   ```bash
   docker-compose ps
   ```

4. **Check Kafka topics:**
   ```bash
   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

5. **Test the platform (see Testing section below)**

### Stopping the Platform

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

---

## 🧪 Testing

### End-to-End Order Flow Test

```bash
# 1. Register a customer
curl -X POST http://localhost:8081/customers \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "phone": "+264811234567"
  }'

# 2. Register a restaurant
curl -X POST http://localhost:8082/restaurants \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Pizza Palace",
    "description": "Best pizza in Windhoek",
    "cuisine": ["Italian", "Pizza"],
    "address": "123 Independence Ave, Windhoek"
  }'

# 3. Add menu items
curl -X POST http://localhost:8082/restaurants/{restaurantId}/menu \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Margherita Pizza",
    "description": "Classic tomato and mozzarella",
    "price": 95.00,
    "category": "Pizza",
    "isAvailable": true,
    "preparationTime": 20
  }'

# 4. Place an order
curl -X POST http://localhost:8083/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "{customerId}",
    "restaurantId": "{restaurantId}",
    "items": [
      {
        "menuItemId": "{menuItemId}",
        "name": "Margherita Pizza",
        "quantity": 2,
        "unitPrice": 95.00
      }
    ],
    "deliveryAddress": "456 Sam Nujoma Drive, Windhoek"
  }'

# 5. Check order status
curl http://localhost:8083/orders/{orderId}

# 6. Track order lifecycle
curl http://localhost:8083/orders/{orderId}/track

# 7. Check payment status
curl http://localhost:8084/payments/order/{orderId}

# 8. Check delivery status
curl http://localhost:8085/deliveries/order/{orderId}

# 9. View notifications
curl http://localhost:8086/notifications/recipient/{customerId}

# 10. View admin dashboard
curl http://localhost:8087/admin/dashboard
```

### Automated Testing Script

For automated end-to-end testing, use the provided test scripts:

**Linux/Mac:**
```bash
chmod +x test-platform.sh
./test-platform.sh
```

**Windows:**
```cmd
test-platform.bat
```

The automated test script will:
1. Register a customer and add delivery address
2. Register a restaurant and add menu items
3. Register a driver and set availability
4. Place an order
5. Verify payment processing
6. Track order status changes through the lifecycle
7. Verify delivery assignment
8. Check notifications and admin dashboard

---

## 📁 Project Structure

```
distributed-food-delivery-platform/
│
├── docker-compose.yml              # Docker Compose orchestration
├── README.md                       # Project documentation
├── test-platform.sh                # Automated test script (Linux/Mac)
├── test-platform.bat               # Automated test script (Windows)
│
├── mongo-init/                     # MongoDB initialization
│   └── init-mongo.js               # Database & index creation
│
├── customer-service/               # Customer microservice
│   ├── Ballerina.toml
│   ├── Config.toml
│   ├── main.bal
│   └── Dockerfile
│
├── restaurant-service/             # Restaurant microservice
│   ├── Ballerina.toml
│   ├── Config.toml
│   ├── main.bal
│   └── Dockerfile
│
├── order-service/                  # Order microservice (core)
│   ├── Ballerina.toml
│   ├── Config.toml
│   ├── main.bal
│   └── Dockerfile
│
├── payment-service/                # Payment microservice
│   ├── Ballerina.toml
│   ├── Config.toml
│   ├── main.bal
│   └── Dockerfile
│
├── delivery-service/               # Delivery microservice
│   ├── Ballerina.toml
│   ├── Config.toml
│   ├── main.bal
│   └── Dockerfile
│
├── notification-service/           # Notification microservice
│   ├── Ballerina.toml
│   ├── Config.toml
│   ├── main.bal
│   └── Dockerfile
│
└── admin-service/                  # Admin/reporting microservice
    ├── Ballerina.toml
    ├── Config.toml
    ├── main.bal
    └── Dockerfile
```

---

## ✅ Implementation Status

### Completed Features

- ✅ All 7 microservices implemented (Customer, Restaurant, Order, Payment, Delivery, Notification, Admin)
- ✅ RESTful API endpoints for all services
- ✅ Apache Kafka event-driven communication
- ✅ Order lifecycle state machine with valid transitions
- ✅ Kafka consumer logic implemented for:
  - Order Service: Consumes payments.completed, delivery.assigned, delivery.completed
  - Delivery Service: Consumes orders.confirmed, orders.cancelled
  - Restaurant Service: Consumes orders.created
  - Customer Service: Consumes orders.created
  - Notification Service: Consumes all order, payment, and delivery events
  - Admin Service: Consumes all events for analytics
- ✅ Docker containerization for all services
- ✅ Docker Compose orchestration
- ✅ MongoDB initialization scripts
- ✅ Driver auto-assignment logic with distance calculation
- ✅ Payment processing simulation (90% success rate)
- ✅ Multi-channel notification system (Email, SMS, Push, In-App)
- ✅ Admin dashboard with real-time analytics
- ✅ Automated test scripts (Linux/Mac and Windows)

### Key Design Decisions

1. **In-Memory Data Storage**: Services use in-memory maps for data persistence (simulating MongoDB) for simplicity and faster testing
2. **Event-Driven Architecture**: All service-to-service communication happens through Kafka topics
3. **Service Independence**: Each service has its own database schema and business logic
4. **Fault Tolerance**: Services can operate independently; Kafka provides message durability
5. **Scalability**: Horizontal scaling through Kafka partitioning and Docker replicas

### Future Enhancements

- [ ] Replace in-memory storage with actual MongoDB connections
- [ ] Add authentication and authorization
- [ ] Implement circuit breakers for service resilience
- [ ] Add comprehensive error handling and retry logic
- [ ] Implement distributed tracing
- [ ] Add metrics and monitoring (Prometheus, Grafana)
- [ ] Implement API rate limiting
- [ ] Add webhooks for external integrations

---

## 👥 Team Members

| # | Name | Student Number | Role |
|---|------|---------------|------|
| 1 | Dyrall Beukes |  | Customer Service & Infrastructure |
| 2 | Mazi de Klerk |  | Restaurant Service & Database |
| 3 | Junior Steyn |  | Order Service & Payment Service |
| 4 | Aden Beukes |  | Delivery Service & Testing |
| 5 | Grace Urikos |  | Notification & Admin Service & Documentation |

