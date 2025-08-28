import os
import pandas as pd
import sqlalchemy as sa
import yaml

CONFIG = yaml.safe_load(open("/quick-commerece-pipeline/config/app_config.yaml"))
RAW_DIR = CONFIG["paths"]["raw_dir"]
os.makedirs(RAW_DIR, exist_ok=True)

def main():
    mysql = CONFIG["mysql"]
    url = sa.engine.URL.create(
        "mysql+pymysql",
        username=mysql["user"],
        password=mysql["password"],
        host=mysql["host"],
        port=mysql["port"],
        database=mysql["database"],
    )
    engine = sa.create_engine(url)

    df = pd.read_sql("SELECT * FROM orders", engine)
    out = os.path.join(RAW_DIR, "orders.csv")
    df.to_csv(out, index=False)
    print(f"Exported {len(df)} orders → {out}")

if __name__ == "__main__":
    main()
