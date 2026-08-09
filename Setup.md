# Kafka Setup Guide
This file contains the one-time local installation and configuration steps for Apache Kafka used by this project.
For the project overview, features, architecture, and normal application startup instructions, see [`README.md`](./README.md).
---

## 1. Environment

The project was developed with:

- Java 21 LTS
- Apache Kafka 4.3.1
- Scala build 2.13
- Kafka package: `kafka_2.13-4.3.1`
- Windows
- PowerShell
- IntelliJ IDEA
- Gradle

Kafka runs in **KRaft mode**.
ZooKeeper is **not used**.
---

## 2. Why There Is No ZooKeeper

The Kafka version used by this project is:

kafka_2.13-4.3.1

Kafka 4.x uses KRaft for metadata management and does not require ZooKeeper.

Old architecture:

Kafka Broker
    |
    v
ZooKeeper

Architecture used by this project:

Kafka Broker
    |
    v
KRaft Controller

Therefore, files such as the following are not expected:
zookeeper-server-start.bat
zookeeper.properties
---

## 3. Install Java 21

Check Java:

powershell
java -version

Expected output:
openjdk version "21..."

If Java is not installed:

powershell
winget install Microsoft.OpenJDK.21

After installation, close PowerShell, open it again, and verify:

powershell
java -version
---

## 4. Download and Extract Kafka

Download:
kafka_2.13-4.3.1.tgz
Extract the archive.

Move or rename the extracted directory so that Kafka is located at:
C:\kafka
The final structure should look similar to:
C:\kafka
│
├── bin
│   └── windows
│       ├── kafka-storage.bat
│       ├── kafka-server-start.bat
│       ├── kafka-server-stop.bat
│       ├── kafka-topics.bat
│       ├── kafka-console-producer.bat
│       └── kafka-console-consumer.bat
│
├── config
│   └── server.properties
│
├── libs
├── licenses
├── site-docs
├── LICENSE
└── NOTICE

Because this project is run on Windows, commands use the `.bat` scripts under:

C:\kafka\bin\windows
---

## 5. Create the Kafka Data Directory

Create:
C:\kafka-data
PowerShell:
powershell
mkdir C:\kafka-data
The important directories are now:
C:\kafka
C:\kafka-data
---

## 6. Configure `server.properties`

Open:
C:\kafka\config\server.properties
Find:
properties
log.dirs=/tmp/kraft-combined-logs
Change it to:
properties
log.dirs=C:/kafka-data
The KRaft configuration should remain enabled.
Typical relevant settings are:
properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
The application connects to the Kafka broker through:
localhost:9092
The KRaft controller uses:
localhost:9093
---

## 7. Initialize the KRaft Cluster

These steps are performed only once.
Open PowerShell:
powershell
cd C:\kafka
Generate a cluster UUID:
powershell
.\bin\windows\kafka-storage.bat random-uuid
Example:
4L6g3nShT-eMCtK--X86sw
Use the generated value in the next command:
powershell
.\bin\windows\kafka-storage.bat format --standalone -t YOUR_CLUSTER_ID -c .\config\server.properties
Example:
powershell
.\bin\windows\kafka-storage.bat format --standalone -t 4L6g3nShT-eMCtK--X86sw -c .\config\server.properties
This initializes the Kafka metadata and data directory.
### Important
Do not generate a new cluster ID or format the storage every time Kafka starts.
Do not normally run these again:
powershell
.\bin\windows\kafka-storage.bat random-uuid
powershell
.\bin\windows\kafka-storage.bat format ...
---
## 8. Windows WMIC Workaround

On the Windows machine used for this project, Kafka startup initially produced:
'wmic' is not recognized as an internal or external command
The workaround used in this project was to explicitly set Kafka heap options before starting the broker:
powershell
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
Verify:
powershell
$env:KAFKA_HEAP_OPTS
Expected:
-Xmx1G -Xms1G
This value is set for the current PowerShell session.
---

## 9. Start Kafka for the First Test

Run:

powershell
cd C:\kafka
powershell
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
Keep this PowerShell window open.

---
## 10. Verify Port 9092
Open another PowerShell:
powershell
Test-NetConnection localhost -Port 9092
Expected:
TcpTestSucceeded : True
---

## 11. Create the Project Topic
The actual project topic is:
order-created
Create it once:
powershell
cd C:\kafka
powershell
.\bin\windows\kafka-topics.bat --create --topic order-created --bootstrap-server=localhost:9092 --partitions 1 --replication-factor 1
List topics:
powershell
.\bin\windows\kafka-topics.bat --bootstrap-server=localhost:9092 --list
Expected to include:
order-created
A temporary `test-topic` may also exist from the original Kafka installation test. It is not required by the application.
---
## 12. Optional Console Test

A Kafka console producer and consumer can be used to verify the broker manually.

Producer:
powershell
cd C:\kafka
powershell
.\bin\windows\kafka-console-producer.bat --bootstrap-server=localhost:9092 --topic order-created
Consumer:
powershell
cd C:\kafka
powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server=localhost:9092 --topic order-created --from-beginning
If messages typed into the producer appear in the consumer, Kafka is working correctly.
---
## 13. Common Setup Problems

### `wmic is not recognized`

Run:
powershell
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
then start Kafka again.

### PowerShell cannot find a Kafka script

Check the current directory.

Wrong:
PS C:\Users\username>
Correct:
PS C:\kafka>
Move to Kafka:
powershell
cd C:\kafka
### Wrong consumer script name

Wrong:
kafka-console-customer.bat
Correct:
kafka-console-consumer.bat

### Kafka is not listening on 9092

Check:

powershell
Test-NetConnection localhost -Port 9092
If it returns:
TcpTestSucceeded : False
restart Kafka and inspect the broker logs.
---

## 14. Normal Kafka Startup After Installation

After the one-time installation has been completed, Kafka only needs these commands:

powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
.\bin\windows\kafka-server-start.bat .\config\server.properties

Do not format the cluster again.

After Kafka is running, continue with the normal project startup instructions in:

README.md
