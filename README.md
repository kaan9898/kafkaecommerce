# Kafka E-Commerce Microservices

> **First-time setup:** Before running this project for the first time, follow [`KAFKA_SETUP.md`](./KAFKA_SETUP.md) to install and configure Apache Kafka 4.3.1 in KRaft mode.

This README describes the project itself, its current features, architecture, Kafka communication flow, configuration, and the normal steps required to run the application after Kafka has already been installed.

---

## Project Overview

This project demonstrates event-driven communication between Spring Boot microservices using Apache Kafka.

Current working flow:

```text
Bruno / REST Client
        |
        | POST /order
        v
+----------------------+
|     Order Service    |
|      port: 8080      |
+----------------------+
        |
        | OrderCreatedEvent
        v
+----------------------+
|    Apache Kafka      |
| localhost:9092       |
| topic: order-created |
+----------------------+
        |
        v
+----------------------+
|  Inventory Service   |
|      port: 8081      |
+----------------------+
        |
        v
Order is processed by the consumer
```

The planned full architecture is:

```text
REST
 |
 v
Order Service
 |
 | Kafka: order-created
 v
------------------------------
|                            |
v                            v
Inventory Service      Notification Service
 |
 | Kafka: inventory-result
 v
Order Service
```

---

## Technologies

- Java 21 LTS
- Spring Boot
- Spring Kafka
- Gradle
- Apache Kafka 4.3.1
- Kafka KRaft mode
- IntelliJ IDEA
- Bruno
- Windows / PowerShell

Kafka broker:

```text
localhost:9092
```

Current Kafka topic:

```text
order-created
```

ZooKeeper is not used.

PostgreSQL is not currently required by this stage of the project.

---

## Current Features

The project currently supports:

- REST order creation
- Automatic UUID generation for each order
- Automatic order creation timestamp
- Publishing an `OrderCreatedEvent` to Kafka
- JSON serialization of Kafka events
- Kafka consumer in Inventory Service
- JSON deserialization into `OrderCreatedEvent`
- Separate Order and Inventory microservices
- Kafka consumer groups
- Manual Kafka verification using console tools
- REST testing through Bruno

---

# Services

## Order Service

Port:

```text
8080
```

Current endpoint:

```http
POST /order
```

Example:

```text
http://localhost:8080/order
```

Request:

```json
{
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2
}
```

The Order Service generates:

```text
orderId
createdTime
```

Example response:

```json
{
  "orderId": "d57cc29a-8d24-4509-a47b-1b07055a80a6",
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2,
  "createdTime": "2026-08-09T20:15:48.096..."
}
```

Expected HTTP status:

```text
201 Created
```

After creating the order, Order Service publishes an event to:

```text
order-created
```

---

## Inventory Service

Port:

```text
8081
```

Inventory Service listens to:

```text
order-created
```

Consumer group:

```text
inventory-service
```

Expected console output after receiving an order:

```text
Processing order:
OrderId: ...
Product: Laptop
Quantity: 2
```

---

# Event Model

The Kafka event is represented by an `OrderCreatedEvent`.

Example structure:

```java
public record OrderCreatedEvent(
        UUID orderId,
        Long customerId,
        String product,
        Integer quantity,
        LocalDateTime createdTime
) {
}
```

Example event payload:

```json
{
  "orderId": "d57cc29a-8d24-4509-a47b-1b07055a80a6",
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2,
  "createdTime": "2026-08-09T20:15:48.096..."
}
```

---

# Kafka Producer

The Order Service is the producer for `order-created`.

Example:

```java
@Component
public class OrderProducer {

    private static final String TOPIC = "order-created";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderProducer(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderCreatedEvent(OrderCreatedEvent event) {
        kafkaTemplate.send(
                TOPIC,
                event.orderId().toString(),
                event
        );
    }
}
```

Kafka message:

```text
topic -> order-created
key   -> orderId
value -> OrderCreatedEvent
```

The topic name must match the consumer topic exactly.

---

# Kafka Consumer

Inventory Service consumes `OrderCreatedEvent`.

Example:

```java
@Component
public class OrderConsumer {

    @KafkaListener(
            topics = "order-created",
            groupId = "inventory-service"
    )
    public void consume(OrderCreatedEvent orderCreatedEvent) {

        System.out.println("Processing order: ");
        System.out.println("OrderId: " + orderCreatedEvent.orderId());
        System.out.println("Product: " + orderCreatedEvent.product());
        System.out.println("Quantity: " + orderCreatedEvent.quantity());
    }
}
```

A successful Kafka subscription may produce a log similar to:

```text
partitions assigned: [order-created-0]
```

---

# Order Service Configuration

Example:

```properties
spring.application.name=order-service
server.port=8080

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer

spring.kafka.producer.properties[spring.json.add.type.headers]=false

app.kafka.order-created-topic=order-created
```

Producer value serialization:

```text
OrderCreatedEvent
       |
       | JacksonJsonSerializer
       v
Kafka JSON
```

---

# Inventory Service Configuration

Example:

```properties
spring.application.name=inventory-service
server.port=8081

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.group-id=inventory-service
spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JacksonJsonDeserializer

spring.kafka.consumer.properties[spring.json.use.type.headers]=false
spring.kafka.consumer.properties[spring.json.value.default.type]=com.kaan9898.inventoryservice.dto.OrderCreatedEvent
spring.kafka.consumer.properties[spring.json.trusted.packages]=com.kaan9898.inventoryservice.dto
```

Consumer deserialization:

```text
Kafka JSON
    |
    | JacksonJsonDeserializer
    v
OrderCreatedEvent
```

---

# Gradle Dependencies

Kafka integration uses the Spring Boot Kafka starter:

```gradle
implementation 'org.springframework.boot:spring-boot-starter-kafka'
```

The project also uses Spring MVC and validation.

Typical relevant dependencies:

```gradle
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-kafka'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-webmvc'

    testImplementation 'org.springframework.boot:spring-boot-starter-kafka-test'
    testImplementation 'org.springframework.boot:spring-boot-starter-validation-test'
    testImplementation 'org.springframework.boot:spring-boot-starter-webmvc-test'

    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}
```

PostgreSQL/JPA are not currently required for the implemented Kafka task.

---

# How to Run the Project

If Kafka has not been installed yet, stop here and follow:

```text
Setup.md
```

Once the one-time setup is complete, use the following steps every time you run the project.

---

## Step 1 - Start Kafka

Open PowerShell:

```powershell
cd C:\kafka
```

Set the heap options:

```powershell
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
```

Start the broker:

```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Keep this PowerShell window open.

Do **not** run `random-uuid` or `format` during normal startup.

---

## Step 2 - Verify Kafka

Open another PowerShell:

```powershell
Test-NetConnection localhost -Port 9092
```

Expected:

```text
TcpTestSucceeded : True
```

---

## Step 3 - Verify the Topic

Run:

```powershell
cd C:\kafka
```

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server=localhost:9092 --list
```

Make sure the list contains:

```text
order-created
```

If the topic does not exist and this is the first application run after setup:

```powershell
.\bin\windows\kafka-topics.bat --create --topic order-created --bootstrap-server=localhost:9092 --partitions 1 --replication-factor 1
```

---

## Step 4 - Start Inventory Service

In IntelliJ, run:

```text
InventoryServiceApplication
```

Inventory Service should start on:

```text
localhost:8081
```

It should connect to Kafka and subscribe to:

```text
order-created
```

A successful assignment can look similar to:

```text
partitions assigned: [order-created-0]
```

Keep Inventory Service running.

---

## Step 5 - Start Order Service

In IntelliJ, run:

```text
OrderServiceApplication
```

Order Service should start on:

```text
localhost:8080
```

Keep it running.

At this point, the running components should be:

```text
Kafka             -> localhost:9092
Order Service     -> localhost:8080
Inventory Service -> localhost:8081
```

---

## Step 6 - Send an Order with Bruno

Open Bruno.

Method:

```text
POST
```

URL:

```text
http://localhost:8080/order
```

Header:

```text
Content-Type: application/json
```

Body:

```json
{
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2
}
```

Send the request.

Expected HTTP response:

```text
201 Created
```

Expected JSON response:

```json
{
  "orderId": "...",
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2,
  "createdTime": "..."
}
```

---

## Step 7 - Check Inventory Service

After Bruno receives `201 Created`, open the Inventory Service console in IntelliJ.

Expected output:

```text
Processing order:
OrderId: ...
Product: Laptop
Quantity: 2
```

This confirms:

```text
Bruno
   |
   v
Order Service
   |
   v
Kafka / order-created
   |
   v
Inventory Service
```

is working.

---

# Manual Kafka Message Verification

If Bruno returns `201 Created` but Inventory Service does not print the order, verify Kafka directly.

Open PowerShell:

```powershell
cd C:\kafka
```

Run:

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server=localhost:9092 --topic order-created --from-beginning
```

Send another order from Bruno.

If the JSON event appears in this console, then:

```text
Order Service -> Kafka
```

is working.

The remaining issue is then on the Inventory consumer/deserializer side.

---

# Problems Found During Development

## Wrong Kafka Topic

At one point, the producer used:

```java
private static final String TOPIC = "order-service";
```

while Inventory listened to:

```text
order-created
```

Result:

```text
Producer -> order-service
Consumer -> order-created
```

Inventory therefore received nothing.

Correct configuration:

```java
private static final String TOPIC = "order-created";
```

---

## Wrong JSON Deserializer

Incorrect:

```properties
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.JsonDeserializer
```

Correct for this project:

```properties
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JacksonJsonDeserializer
```

---

## Wrong Producer Serializer

A producer must use a serializer.

Correct:

```properties
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer
```

Using an incorrect class caused the REST request to return:

```text
500 Internal Server Error
```

After the serializer was corrected, Bruno returned:

```text
201 Created
```

---

# Troubleshooting

## Bruno returns `500 Internal Server Error`

Check the `OrderServiceApplication` console.

The problem is normally inside the Spring Boot application rather than Bruno.

Look for:

```text
Exception
```

or:

```text
Caused by:
```

---

## Bruno cannot connect

Verify Order Service is running:

```text
localhost:8080
```

Also verify the correct endpoint:

```text
POST /order
```

---

## Inventory receives nothing

Check:

1. Kafka is running.
2. `localhost:9092` is reachable.
3. `order-created` exists.
4. Order Producer publishes to `order-created`.
5. Inventory consumer listens to `order-created`.
6. Inventory is assigned `order-created-0`.
7. JSON serializer/deserializer configuration is correct.
8. Manual Kafka console consumer can see the event.

---

# Stopping the Project

Stop Order Service from IntelliJ.

Stop Inventory Service from IntelliJ.

Stop Kafka by selecting the Kafka PowerShell window and pressing:

```text
Ctrl + C
```

---

# Current Project Status

Currently completed:

- Apache Kafka integration
- Kafka 4.3.1
- KRaft mode
- No ZooKeeper
- Order REST endpoint
- UUID order ID generation
- Creation timestamp generation
- `OrderCreatedEvent`
- JSON Kafka serialization
- `order-created` producer
- Inventory Kafka consumer
- JSON Kafka deserialization
- Kafka consumer group
- Bruno REST testing
- `201 Created` response
- Kafka topic mismatch troubleshooting
- Serializer/deserializer troubleshooting

Not currently required:

- PostgreSQL
- JPA persistence
- ZooKeeper

---

# Planned Next Steps

Possible next tasks:

1. Create `inventory-result`.
2. Publish inventory processing results from Inventory Service.
3. Consume `inventory-result` in Order Service.
4. Add Notification Service.
5. Let Notification Service independently consume `order-created`.
6. Add error handling and retry logic.
7. Add automated tests if required.
8. Add PostgreSQL later if persistence becomes part of the requirements.

---

# Quick Run

Kafka:

```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Inventory:

```text
InventoryServiceApplication -> Run
```

Order:

```text
OrderServiceApplication -> Run
```

Bruno:

```http
POST http://localhost:8080/order
```

```json
{
  "customerId": 15,
  "product": "Laptop",
  "quantity": 2
}
```

Expected:

```text
201 Created
```

Inventory console:

```text
Processing order:
OrderId: ...
Product: Laptop
Quantity: 2
```

