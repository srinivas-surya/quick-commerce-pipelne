import os
import yaml
from pymongo import MongoClient
import pandas as pd

CONFIG = yaml.safe_load(open("/quick-commerece-pipeline/config/app_config.yaml"))
RAW_DIR = CONFIG["paths"]["raw_dir"]
os.makedirs(RAW_DIR, exist_ok=True)

def main():
    mongo = CONFIG["mongo"]
    client = MongoClient(host=mongo["host"], port=mongo["port"])
    coll = client[mongo["database"]][mongo["collection"]]

    docs = list(coll.find({}, {"_id": 0}))
    df = pd.DataFrame(docs)
    out = os.path.join(RAW_DIR, "inventory.json")
    df.to_json(out, orient="records", lines=False)
    print(f"Exported {len(df)} inventory docs → {out}")

if __name__ == "__main__":
    main()
