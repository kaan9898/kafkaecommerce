# Kafka E-Commerce Microservices

An event-driven e-commerce microservices project built with **Java 21, Spring Boot, Apache Kafka 4.3.1, and PostgreSQL**.

This project demonstrates asynchronous communication between independent microservices using Apache Kafka. It covers the order lifecycle, inventory processing, customer notifications, reliable message delivery, error handling, idempotent consumers, Kafka partitioning, consumer groups, integration testing, and observability.

The system consists of three microservices:

- **Order Service** – Creates and manages orders.
- **Inventory Service** – Processes inventory requests and publishes processing results.
- **Notification Service** – Independently consumes order events and simulates customer notifications.

The project was developed incrementally, starting with basic Kafka producer-consumer communication and progressing toward more advanced distributed systems concepts.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Microservices](#microservices)
- [Event-Driven Communication](#event-driven-communication)
- [Kafka Topics](#kafka-topics)
- [Order Processing Flow](#order-processing-flow)
- [Reliability and Fault Tolerance](#reliability-and-fault-tolerance)
- [Kafka Concepts](#kafka-concepts)
- [Database and Messaging Consistency](#database-and-messaging-consistency)
- [Observability](#observability)
- [Testing](#testing)
- [Installation and Setup](#installation-and-setup)
- [Running the Application](#running-the-application)
- [Implementation Roadmap](#implementation-roadmap)
- [Project Status](#project-status)

---

## Architecture Overview

The application follows an **Event-Driven Architecture (EDA)**. Microservices communicate asynchronously through Kafka topics instead of depending on direct synchronous communication with one another.

Each service has a separate responsibility and consumes only the events relevant to its business logic.

```text
                         REST Client
                             |
                             v
                       Order Service
                             |
                             v
                         PostgreSQL
                             |
                             v
                  Kafka: order-created
                             |
                 +-----------+-----------+
                 |                       |
                 v                       v
         Inventory Service      Notification Service
                 |                       |
                 v                       v
        Stock Availability        Email Simulation
             Processing
                 |
                 v
              Kafka
          inventory-result
                 |
                 v
            Order Service
                 |
                 v
         Update Order Status
           in PostgreSQL
```

**Important architecture details:**

- Order Service creates and persists orders.
- Order Service publishes an `order-created` event.
- Inventory Service consumes the event and processes stock availability.
- Notification Service independently consumes the same event.
- Inventory Service publishes an `inventory-result` event.
- Order Service consumes the inventory result and updates the order status.
- Each consumer group processes events independently.

This architecture demonstrates loose coupling, asynchronous processing, and event fan-out.

---

## Technology Stack

| Category | Technology |
|---|---|
| Programming Language | Java 21 |
| Application Framework | Spring Boot |
| Messaging Framework | Spring Kafka |
| Message Broker | Apache Kafka 4.3.1 |
| Kafka Mode | KRaft |
| Database | PostgreSQL |
| Build Tool | Gradle |
| REST API Testing | Bruno |
| IDE | IntelliJ IDEA |
| Operating Environment | Windows / PowerShell |
| Metrics | Micrometer |
| Monitoring | Prometheus |
| Integration Testing | Embedded Kafka or Testcontainers |

Kafka runs in KRaft mode. ZooKeeper is not used.

For the initial Kafka installation and configuration, see [Setup.md](./Setup.md).

---

## Microservices

### 1. Order Service

The Order Service is responsible for order creation and order lifecycle management.

**Responsibilities:**

- Exposes a REST endpoint for creating orders.
- Validates incoming requests using Bean Validation.
- Generates a unique order ID using UUID.
- Generates an order creation timestamp.
- Persists orders in PostgreSQL.
- Publishes order creation events to Kafka after successful persistence.
- Consumes inventory processing results.
- Updates order status in the database.
- Uses Kafka producer and consumer functionality.

**Endpoint:**

```http
POST /order
```

Example URL:

```text
http://localhost:8080/order
```

Example request:

```json
{
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2
}
```

The service generates the order ID and creation timestamp.

The order is saved before the corresponding creation event is published.

### 2. Inventory Service

The Inventory Service handles inventory-related order processing.

**Responsibilities:**

- Consumes `order-created` events.
- Processes incoming order information.
- Simulates stock availability.
- Checks whether the requested quantity can be fulfilled.
- Publishes inventory processing results.
- Sends results through the `inventory-result` topic.
- Handles processing failures through retry mechanisms.
- Sends permanently failed messages to a Dead Letter Topic.
- Prevents duplicate event processing through idempotency.

**Main Kafka topics:**

```text
Consumes: order-created
Publishes: inventory-result
```

### 3. Notification Service

The Notification Service handles customer notification processing independently of inventory operations.

**Responsibilities:**

- Consumes `order-created` events.
- Uses an independent Kafka consumer group.
- Simulates sending email notifications.
- Processes order events independently of Inventory Service.
- Demonstrates Kafka Publish/Subscribe communication.
- Demonstrates event fan-out through multiple consumer groups.

**Main Kafka topic:**

```text
Consumes: order-created
```

Because Notification Service uses a separate consumer group, it receives its own copy of the events rather than competing with Inventory Service for messages.

---

## Event-Driven Communication

The system uses Kafka events to coordinate communication between services.

### OrderCreatedEvent

Published by Order Service when a new order is successfully persisted.

The event contains order-related information, including:

- Order ID
- Customer ID
- Product
- Quantity
- Creation timestamp

Example event payload:

```json
{
  "orderId": "d57cc29a-8d24-4509-a47b-1b07055a80a6",
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2,
  "createdTime": "2026-08-09T20:15:48.096"
}
```

### InventoryResultEvent

Published by Inventory Service after processing an order.

It communicates the result of the inventory operation to Order Service so that the order status can be updated.

The event-driven approach allows Inventory Service to process orders without directly calling Order Service.

### Multiple Event Types

The application also explores handling multiple event types through Kafka messaging, allowing consumers to distinguish and process different business events.

---

## Kafka Topics

| Topic | Purpose |
|---|---|
| `order-created` | Publishes newly created order events. |
| `inventory-result` | Publishes inventory processing results. |
| Dead Letter Topic | Stores messages that cannot be processed after configured retries. |

The exact DLT topic name follows the application's Kafka configuration.

---

## Order Processing Flow

The complete order lifecycle follows these steps:

1. A client sends an order creation request to Order Service.
2. Order Service validates the incoming request.
3. A unique order ID and creation timestamp are generated.
4. The order is saved in PostgreSQL.
5. Order Service publishes an `order-created` event.
6. Inventory Service consumes the event.
7. Inventory Service simulates stock availability and processes the requested quantity.
8. Inventory Service publishes an `inventory-result` event.
9. Order Service consumes the inventory result.
10. Order Service updates the order status in PostgreSQL.
11. Notification Service independently consumes the original `order-created` event.
12. Notification Service simulates sending an email notification.

This flow demonstrates asynchronous communication between independent services.

---

## Reliability and Fault Tolerance

The project implements several mechanisms to improve message processing reliability.

### Retry Mechanism

Temporary processing failures are handled through retry attempts.

- Simulates random processing exceptions.
- Retries failed message processing.
- Uses configured backoff behavior.
- Allows temporary failures to recover without immediately losing messages.

### Dead Letter Topic (DLT)

Messages that cannot be processed after the configured retry attempts are forwarded to a Dead Letter Topic.

The implementation uses:

- `DeadLetterPublishingRecoverer`
- Retry and recovery configuration
- A dedicated DLT consumer

This separates permanently failed messages from normal processing and makes failed events available for investigation.

### Idempotency

Kafka messages may be delivered more than once. The project implements idempotent processing to prevent duplicate business operations.

- Uses unique event identification.
- Tracks processed events in the database.
- Detects previously processed messages.
- Prevents repeated processing of the same event.

This is particularly important when consumers retry messages or receive duplicate deliveries.

---

## Kafka Concepts

### Message Keys and Partitions

The project uses `customerId` as the Kafka message key to explore key-based partition assignment.

Messages with the same key are routed to the same partition under a stable partitioning configuration.

This allows the application to demonstrate Kafka's ordering guarantees within a partition.

### Consumer Groups

The project explores consumer groups by running multiple instances of a consumer service.

**Load balancing:**

- Runs three instances within the same consumer group.
- Kafka distributes partition assignments across available consumers.
- Consumers process different partitions in parallel when enough partitions are available.

**Publish/Subscribe:**

- Uses separate consumer groups for independent services.
- Inventory Service and Notification Service consume the same `order-created` topic.
- Both services receive and process their own copy of each event.

This demonstrates the difference between load balancing within a consumer group and independent consumption across multiple groups.

### Multiple Event Types

The project also explores handling multiple event types through Kafka, allowing consumers to identify and process different business events.

---

## Database and Messaging Consistency

The Order Service persists order information in PostgreSQL before publishing the corresponding Kafka event.

This introduces an important distributed systems challenge: the **dual-write problem**.

A database transaction and a Kafka publish operation are separate operations. A failure between them can cause the database and Kafka to become inconsistent.

The project explores:

- Database transaction boundaries.
- The dual-write problem.
- Database and message publishing consistency.
- The Transactional Outbox Pattern.
- Strategies for reliable event publishing.

**Note:** The Transactional Outbox Pattern is explored as a consistency solution; this README does not claim that a complete Outbox implementation is part of the project.

---

## Observability

The project includes application monitoring and logging capabilities.

**Implemented concepts:**

- Micrometer metrics.
- Prometheus monitoring.
- Application health endpoints.
- Correlation ID-based logging.
- Improved troubleshooting across microservices.

These capabilities help monitor application behavior and investigate requests and events as they move through the system.

---

## Testing

Integration tests are used to verify Kafka communication and event processing.

The testing scope includes:

- Producer event publishing.
- Consumer message processing.
- Kafka communication.
- Asynchronous event workflows.
- Integration between producer and consumer components.

Embedded Kafka or Testcontainers can be used for Kafka integration testing, depending on the test configuration.

---

## Installation and Setup

Before running the application for the first time, install and configure Apache Kafka 4.3.1 in KRaft mode.

Follow:

[Setup.md](./Setup.md)

The setup guide covers the one-time Kafka installation and configuration process.

ZooKeeper is not required.

---

## Running the Application

After completing the initial Kafka setup, use the following steps whenever you want to run the project.

### Step 1 – Start Kafka

Open PowerShell:

```powershell
cd C:\kafka
```

Set Kafka heap options:

```powershell
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
```

Start the Kafka broker:

```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Keep the terminal open while using the application.

Do not run the Kafka storage formatting commands during normal startup.

### Step 2 – Verify Kafka

Open another PowerShell window:

```powershell
Test-NetConnection localhost -Port 9092
```

The expected result is:

```text
TcpTestSucceeded : True
```

### Step 3 – Start the Microservices

Run the three Spring Boot applications independently:

| Service | Application | Port |
|---|---|---|
| Order Service | `OrderServiceApplication` | 8080 |
| Inventory Service | `InventoryServiceApplication` | 8081 |
| Notification Service | `NotificationServiceApplication` | Configured application port |

Make sure Kafka is running before starting the services.

### Step 4 – Send an Order Request

Use Bruno or another REST client.

**Method:**

```http
POST
```

**URL:**

```text
http://localhost:8080/order
```

**Header:**

```text
Content-Type: application/json
```

**Request body:**

```json
{
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2
}
```

### Step 5 – Verify the Processing Flow

Check the service logs to verify:

- Order Service creates and persists the order.
- `order-created` is published to Kafka.
- Inventory Service receives and processes the event.
- Notification Service independently receives the event.
- Inventory Service publishes `inventory-result`.
- Order Service consumes the result and updates the order status.

---

## Manual Kafka Verification

To inspect messages directly, use the Kafka console consumer.

Open PowerShell:

```powershell
cd C:\kafka
```

Consume events from the beginning:

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server=localhost:9092 --topic order-created --from-beginning
```

To inspect inventory results:

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server=localhost:9092 --topic inventory-result --from-beginning
```

These commands can help verify whether events are being published to the expected topics.

---

## Implementation Roadmap

The project was developed through the following 16 tasks:

| # | Task | Description |
|---:|---|---|
| 1 | Kafka Producer | Created Order Service and published order creation events. |
| 2 | Kafka Consumer | Created Inventory Service to consume order events. |
| 3 | Request Validation | Added Bean Validation and HTTP 400 responses. |
| 4 | Database Integration | Integrated PostgreSQL and persisted orders before event publishing. |
| 5 | Inventory Processing | Simulated stock availability and published inventory results. |
| 6 | Order Status Updates | Consumed inventory results and updated order status. |
| 7 | Retry Mechanism | Implemented retries and backoff for processing failures. |
| 8 | Dead Letter Topic | Added DLT recovery and failed-message consumption. |
| 9 | Idempotency | Prevented duplicate event processing using persistent event tracking. |
| 10 | Notification Service | Added independent event consumption and simulated email notifications. |
| 11 | Multiple Event Types | Explored handling different event types through Kafka. |
| 12 | Kafka Keys and Partitions | Used `customerId` as a key and explored partition ordering. |
| 13 | Consumer Groups | Explored multiple consumer instances, load balancing, and fan-out. |
| 14 | Integration Testing | Tested Kafka producer and consumer communication. |
| 15 | Observability | Added Micrometer, Prometheus, health endpoints, and correlation ID logging. |
| 16 | Transactional Messaging | Explored the dual-write problem and Transactional Outbox Pattern. |

---

## Key Learning Outcomes

Through this project, I gained practical experience with:

- Event-Driven Architecture.
- Microservices design and asynchronous communication.
- Apache Kafka producers and consumers.
- Kafka topics, partitions, and message keys.
- Consumer groups and horizontal scaling.
- Publish/Subscribe messaging.
- Multiple event types.
- PostgreSQL integration.
- Spring Boot validation and persistence.
- Retry mechanisms and backoff strategies.
- Dead Letter Topics and failed-message recovery.
- Idempotent message processing.
- Kafka integration testing.
- Micrometer and Prometheus monitoring.
- Correlation IDs and distributed logging.
- Database and messaging consistency.
- The Transactional Outbox Pattern.

---

## Project Status

**All 16 implementation tasks completed.**

The project demonstrates the progression from basic Kafka messaging to a multi-service event-driven e-commerce system, covering asynchronous processing, reliability, scalability, testing, and observability.

**Repository:** [github.com/kaan9898/kafkaecommerce](https://github.com/kaan9898/kafkaecommerce)
