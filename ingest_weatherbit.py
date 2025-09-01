import os
import requests
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

BASE_URL  = os.getenv("url")
DB_USER   = os.getenv("username")
DB_PASS   = os.getenv("password")
DB_NAME   = os.getenv("database")

assert BASE_URL, "Missing 'url' in .env (Weatherbit endpoint)"
assert DB_USER and DB_PASS and DB_NAME, "Missing Mongo creds or database in .env"

MONGO_URI = f"mongodb://{DB_USER}:{DB_PASS}@localhost:27017/{DB_NAME}?authSource={DB_NAME}"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ts_to_iso_utc(ts: int | str) -> str:
    """Convert epoch seconds to ISO-8601 UTC (Z) string."""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_hourly_24(url: str) -> list[dict]:
    """
    Call Weatherbit hourly forecast endpoint.
    Returns a list of ~24 hourly records with location metadata merged in,
    so each record has city_name/country_code/state_code/lat/lon.
    """
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    payload = resp.json()

    meta = {
        "city_name":    payload.get("city_name"),
        "country_code": payload.get("country_code"),
        "state_code":   payload.get("state_code"),
        "lat":          payload.get("lat"),
        "lon":          payload.get("lon"),
    }
    data = payload.get("data", []) or []
    # merge top-level meta into each hourly item
    return [{**rec, **meta} for rec in data]


def normalize(rec: dict) -> dict:
    """
    normalize single record, our staging schema for Mongo.
    Removes bulky 'raw' and sets a deterministic _id for dedupe.
    """
    # unified timestamp: prefer timestamp_utc (already UTC), else datetime/ob_time, else ts(epoch)
    dt_iso: str
    dt_str = rec.get("timestamp_utc") or rec.get("datetime") or rec.get("ob_time")
    if dt_str:
        # ensure the 'Z' suffix
        dt_iso = dt_str.replace(" ", "T")
        if not dt_iso.endswith("Z"):
            dt_iso += "Z"
    elif rec.get("ts") is not None:
        dt_iso = ts_to_iso_utc(rec["ts"])
    else:
        dt_iso = now_utc_iso()

    city = rec.get("city_name") or "UNKNOWN"
    wx   = rec.get("weather") or {}
    desc = wx.get("description") if isinstance(wx, dict) else None

    doc = {
        # PK used by Airbyte dedup in destination
        "_id": f"{city}|{dt_iso}",

        "provider": "weatherbit",

        # identity / location
        "city": city,
        "country": rec.get("country_code") or "",
        "state_code": rec.get("state_code") or "",
        "lat": rec.get("lat"), 
        "lon": rec.get("lon"),

        # business time
        "dt": dt_iso,

        # core metrics
        "temp_c": rec.get("temp"),
        "feels_like_c": rec.get("app_temp"),
        "rh": rec.get("rh"),
        "dewpt_c": rec.get("dewpt"),
        "wind_ms": rec.get("wind_spd"),
        "wind_gust_ms": rec.get("wind_gust_spd"),
        "wind_dir_deg": rec.get("wind_dir"),
        "wind_cdir": rec.get("wind_cdir"),
        "wind_cdir_full": rec.get("wind_cdir_full"),
        "pop_pct": rec.get("pop"),
        "precip_mm": rec.get("precip"),
        "snow_mm": rec.get("snow"),
        "snow_depth_mm": rec.get("snow_depth"),
        "clouds_low_pct": rec.get("clouds_low"),
        "clouds_mid_pct": rec.get("clouds_mid"),
        "clouds_hi_pct": rec.get("clouds_hi"),
        "clouds_pct": rec.get("clouds"),
        "slp_mb": rec.get("slp"),
        "pres_mb": rec.get("pres"),
        "vis_km": rec.get("vis"),
        "uv_index": rec.get("uv"),
        "dhi_wm2": rec.get("dhi"),
        "dni_wm2": rec.get("dni"),
        "ghi_wm2": rec.get("ghi"),
        "solar_rad_wm2": rec.get("solar_rad"),
        "ozone_dobson": rec.get("ozone"),

        # labels
        "conditions": desc,
        "weather_code": wx.get("code") if isinstance(wx, dict) else None,
        "weather_icon": wx.get("icon") if isinstance(wx, dict) else None,
        "pod": rec.get("pod"),

        # incremental fields for Airbyte
        "ingestedAt": now_utc_iso(),
        "updatedAt":  now_utc_iso(),
    }
    return doc


def ensure_indexes(collection):
    """create indexes once (safe to call repeatedly)."""
    try:
        collection.create_index([("updatedAt", 1)])
        collection.create_index([("city", 1), ("dt", 1)], unique=True)
    except Exception as e:
        print(f"[warn] creating indexes: {e}")


def upsert_batch(collection, docs: list[dict]) -> int:
    if not docs:
        return 0
    ops = [UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True) for d in docs]
    res = collection.bulk_write(ops, ordered=False)
    return (res.upserted_count or 0) + (res.modified_count or 0)


def main():
    # connect to Mongo and select collection
    client = MongoClient(MONGO_URI)
    col = client.get_default_database()["weather"]  # e.g., skylogix.weather
    ensure_indexes(col)

    # fetch -> normalize -> upsert
    records = fetch_hourly_24(BASE_URL)
    docs = [normalize(r) for r in records]
    changed = upsert_batch(col, docs)

    print(f"fetched={len(records)}, upserted/modified={changed}")

if __name__ == "__main__":
    main()