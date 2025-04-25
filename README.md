# F1 Telemetry Streaming Pipeline (Kafka → NiFi → MongoDB)

This project streams Formula 1 lap telemetry data in real time using:

- **Kafka** for message streaming
- **Apache NiFi** for data ingestion and transformation
- **MongoDB** for document storage
- **FastF1** for retrieving historical F1 race data

---

## Quickstart

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/f1-telemetry-pipeline.git
cd f1-telemetry-pipeline
```

---

### 2. Launch Services

```bash
cd docker
docker-compose up -d
```

This starts:
- Zookeeper
- Kafka
- Apache NiFi
- MongoDB

---

### 3. Access Apache NiFi

Visit:

```
https://localhost:8443/nifi
```

> **Note**: NiFi uses HTTPS by default on port `8443`.

### Get Username & Password

```bash
docker exec -it nifi bash -c "grep 'Generated Username' /opt/nifi/nifi-current/logs/nifi-app.log"
docker exec -it nifi bash -c "grep 'Generated Password' /opt/nifi/nifi-current/logs/nifi-app.log"
```

Use the values printed in the logs to log in to NiFi.

---

### 4. Import the NiFi Flow

1. Click the **Process Group** icon (4th one) from the top-left toolbar.
2. Drag it onto the canvas.
3. In the pop-up, click **Upload** (right side of the name field).
4. Select and upload the json file:

```
nifi/f1_raw_data.json
```

---

### 5. Enable Kafka and MongoDB Services

1. **Double-click `f1_raw_data`** process group to enter it.
2. **Double-click `ConsumeKafka`**
   - Go to **Properties**
   - Click the 3 dots next to **Kafka Connection Service**
   - Click **Go to service**
   - In `Kafka3ConnectionService`, click the 3 dots → **Enable**
3. Repeat the same for the **MongoDBControllerService**

---

### 6. Start the Flow

- Go back to the main canvas
- Ensure the processors are running (green play buttons)

---

### 7. Stream Lap Telemetry Data

Run the FastF1 Kafka producer:

```bash
python kafka/producer/telemetry_producer.py
```

This script will:
- Fetch historical lap data (e.g., from the 2025 Chinese GP)
- Stream it to Kafka
- Let NiFi process and forward it to MongoDB

---

### 8. Check MongoDB

You can connect to MongoDB on:

```
mongodb://localhost:27017
```

Use MongoDB Compass or the CLI:

```bash
docker exec -it mongodb mongosh
use f1_data
db.laps.find().pretty()
```
