# Weather ETL Pipeline

An **incremental ETL pipeline** for hourly weather forecasts using **Weatherbit API → MongoDB → Airbyte → MotherDuck**.  
The project demonstrates end-to-end data engineering: API ingestion, schema design, staging, incremental sync, and analytics.


##  Pipeline Overview
1. **Extract**: Fetch hourly forecasts from Weatherbit API.  
2. **Transform**: Normalize JSON payloads into a clean schema.  
3. **Load (Staging)**: Upsert into MongoDB with deterministic IDs.  
4. **Replicate**: Use Airbyte for incremental sync (Append + Deduped).  
5. **Warehouse**: Query-ready tables in DuckDB/MotherDuck.  


## Tech Stack 
- **Python** (API ingestion, normalization, MongoDB upsert)  
- **MongoDB** (staging database with replica set + indexes)  
- **Airbyte** (incremental syncs with PK + cursor)  
- **MotherDuck** (analytics warehouse)  
