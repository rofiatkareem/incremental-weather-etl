# Project Summary

Incremental ETL pipeline for hourly weather forecasts:
**Weatherbit API → MongoDB (staging) → Airbyte (replication) → MotherDuck (analytics)**

---

## 1) Prerequisites

- Python 3.10+  
- `pip install -r requirements.txt` (includes: `requests`, `pymongo`, `python-dotenv`) 
- MongoDB running locally
- Weatherbit API key
- Airbyte (**Cloud** or **Self-Hosted**)
- MotherDuck account + service token (for DuckDB in the cloud)

Repo files of interest:
 - `ingest_weatherbit.py` – ETL (fetch → normalize → upsert to Mongo)
 - `README.md` – quick start


## 2) Configure MongoDB (single-node replica set)

- Airbyte’s Mongo source expects a replica set. We use a single node.

1. Edit Mongo config:
   ```bash
   sudo nano /etc/mongod.conf
   ```

    - Add/ensure:

    ```bash
    net:
    bindIp: 0.0.0.0
    replication:
    replSetName: rs0
    ```

- Restart Mongo:

    sudo systemctl restart mongod
    sudo systemctl status mongod


    Initiate the replica set (set host to the IP Airbyte will reach):

    ```bash
    mongosh
    rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "127.0.0.1:27017" }] })
    rs.status()
    ```

- Create project DB + user:

    ```bash
    use skylogix
    db.createUser({
    user: "skyuser",
    pwd: "skypass123",
    roles: [{ role: "readWrite", db: "skylogix" }]
    })
    ```


- (Optional but recommended) Indexes for sync + uniqueness:

    ```bash
    db.weather.createIndex({ updatedAt: 1 })
    db.weather.createIndex({ city: 1, dt: 1 }, { unique: true })
    ```

## 3) Configure .env

Create .env

```bash
url=https://api.weatherbit.io/v2.0/forecast/hourly?city=Lagos,NG&hours=24&key=YOUR_API_KEY
username=skyuser
password=skypass123
database=skylogix
```

## 4) Run the ingestion (Weatherbit → MongoDB)
```bash
python ingest_weatherbit.py

Expected output:

fetched=24, upserted/modified=24
```

## 5) Set up Airbyte

You can use Airbyte Cloud or Self-Hosted. The connection settings are the same.

### Add Source: MongoDB

Cluster type: Self-managed replica set

Connection string:

```python
mongodb://skyuser:skypass123@<HOST>:27017/skylogix?authSource=skylogix&replicaSet=rs0
```


- If Airbyte runs on the same host, use localhost.

- If Airbyte runs in Minikube, use your PC IP


### Add Destination: MotherDuck

MotherDuck:

- Paste MotherDuck API token

- Destination path (DB): md:skylogix

- Schema: public or your defined schema

- Test & Save


### Create the Connection (Mongo → MotherDuck)

Schedule: Manual for first run → then Every 15 min (or hourly)

Run Sync.

## Verify in MotherDuck

MotherDuck (DuckDB CLI):
```sql
SELECT COUNT(*) FROM public.weather;

SELECT city, dt, temp_c, precip_mm
FROM public.weather
ORDER BY dt DESC
LIMIT 10;
