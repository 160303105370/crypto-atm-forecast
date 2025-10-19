import os
import argparse
import sys
from datetime import date, timedelta, datetime
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import boto3

load_dotenv()

# ----- Config from env -----
AWS_REGION           = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET            = os.getenv("S3_BUCKET")
S3_PROCESSED_PREFIX  = os.getenv("S3_PROCESSED_PREFIX", "processed")
S3_LOADS_PREFIX      = os.getenv("S3_LOADS_PREFIX", "loads")
REDSHIFT_WORKGROUP   = os.getenv("REDSHIFT_WORKGROUP")
REDSHIFT_DATABASE    = os.getenv("REDSHIFT_DATABASE", "dev")
REDSHIFT_SCHEMA      = os.getenv("REDSHIFT_SCHEMA", "atm")
REDSHIFT_IAM_ROLE_ARN= os.getenv("REDSHIFT_IAM_ROLE_ARN")
REDSHIFT_SECRET_ARN  = os.getenv("REDSHIFT_SECRET_ARN")  # optional

LOCAL_MERGED = Path("data/data_processed/atm_crypto_merged_daily.csv")
LOCAL_LOADS  = Path("data/data_processed/loads")

def die(msg):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def parse_args():
    p = argparse.ArgumentParser(description="Mini ETL: slice a day from merged CSV, upload to S3, optionally trigger Redshift COPY+INSERT.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", help="ISO date to load (e.g., 2020-05-01)")
    g.add_argument("--next", action="store_true", help="Use one day after the max date currently in Redshift (requires Data API access)")
    return p.parse_args()

def choose_sim_date_from_local():
    df = pd.read_csv(LOCAL_MERGED, parse_dates=["date"])
    dmax = df["date"].max().date()
    print(f"[info] Using LAST date present in local merged dataset: {dmax}")
    return dmax

def choose_next_date_from_redshift():
    client = boto3.client("redshift-data", region_name=AWS_REGION)
    sql = f"select max(date) from {REDSHIFT_SCHEMA}.atm_daily;"
    resp = client.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql,
    )
    # poll for result
    desc = client.describe_statement(Id=resp["Id"])
    while desc["Status"] in ("SUBMITTED","PICKED","STARTED"):
        desc = client.describe_statement(Id=resp["Id"])
    if desc["Status"] != "FINISHED":
        die(f"Data API status={desc['Status']} for MAX(date) query")

    rows = client.get_statement_result(Id=resp["Id"])["Records"]
    if not rows or rows[0][0]["isNull"]:
        die("Redshift atm_daily is empty; cannot infer next date. Load historical first.")
    max_date_str = rows[0][0]["stringValue"]
    max_dt = datetime.strptime(max_date_str, "%Y-%m-%d").date()
    return max_dt + timedelta(days=1)

def slice_day(sim_date: date) -> Path:
    if not LOCAL_MERGED.exists():
        die(f"Missing {LOCAL_MERGED}. Generate it first in your notebook.")

    df = pd.read_csv(LOCAL_MERGED, parse_dates=["date"])
    df["date"] = df["date"].dt.date

    cols = [
        "date","atm_id","atm_city",
        "withdrawals_usd","total_txn_count","income_txn_count","outcome_txn_count",
        "btc_close","btc_pct_change_1d","btc_7d_roc","btc_7d_ma","btc_volatility_7d",
        "is_weekend","payday_flag"
    ]
    cols = [c for c in cols if c in df.columns]

    day_df = df.loc[df["date"] == sim_date, cols]
    if day_df.empty:
        die(f"No rows found for date {sim_date} in merged dataset.")

    LOCAL_LOADS.mkdir(parents=True, exist_ok=True)
    out_path = LOCAL_LOADS / f"atm_forecast_{sim_date}.csv"
    day_df.to_csv(out_path, index=False)
    print(f"[ok] Wrote local daily file: {out_path}")
    return out_path

def upload_to_s3(local_path: Path, s3_bucket: str, s3_prefix: str):
    key = f"{s3_prefix}/{local_path.name}"
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_file(str(local_path), s3_bucket, key)
    print(f"[ok] Uploaded to s3://{s3_bucket}/{key}")
    return key

def run_redshift_copy_insert(s3_key: str):
    if not REDSHIFT_SECRET_ARN:
        # Print SQL for manual run
        print("\n[info] REDSHIFT_SECRET_ARN not set → printing SQL to run in Query Editor v2:")
        bucket_uri = f"s3://{S3_BUCKET}/{s3_key}"
        sql = f"""
                truncate {REDSHIFT_SCHEMA}.stg_load;

                copy {REDSHIFT_SCHEMA}.stg_load
                from '{bucket_uri}'
                iam_role '{REDSHIFT_IAM_ROLE_ARN}'
                csv
                ignoreheader 1
                timeformat 'auto'
                acceptinvchars;

                insert into {REDSHIFT_SCHEMA}.atm_daily
                select
                try_cast(date as date),
                nullif(atm_id,''),
                nullif(atm_city,''),
                nullif(withdrawals_usd,'')::numeric(12,2),
                nullif(total_txn_count,'')::int,
                nullif(income_txn_count,'')::int,
                nullif(outcome_txn_count,'')::int,
                nullif(btc_close,'')::numeric(14,4),
                nullif(btc_pct_change_1d,'')::double precision,
                nullif(btc_7d_roc,'')::double precision,
                nullif(btc_7d_ma,'')::numeric(14,4),
                nullif(btc_volatility_7d,'')::double precision,
                case lower(coalesce(is_weekend,'')) when 'true' then true when '1' then true else false end,
                case lower(coalesce(payday_flag,'')) when 'true' then true when '1' then true else false end
                from {REDSHIFT_SCHEMA}.stg_load
                where not exists (
                select 1 from {REDSHIFT_SCHEMA}.atm_daily d
                where d.date = try_cast({REDSHIFT_SCHEMA}.stg_load.date as date)
                    and d.atm_id = {REDSHIFT_SCHEMA}.stg_load.atm_id
                );
            """
        print(sql)
        return

    client = boto3.client("redshift-data", region_name=AWS_REGION)
    bucket_uri = f"s3://{S3_BUCKET}/{s3_key}"

    statements = [
        f"truncate {REDSHIFT_SCHEMA}.stg_load;",
        f"""copy {REDSHIFT_SCHEMA}.stg_load
            from '{bucket_uri}'
            iam_role '{REDSHIFT_IAM_ROLE_ARN}'
            csv ignoreheader 1 timeformat 'auto' acceptinvchars;""",
        f"""insert into {REDSHIFT_SCHEMA}.atm_daily
            select
              try_cast(date as date),
              nullif(atm_id,''),
              nullif(atm_city,''),
              nullif(withdrawals_usd,'')::numeric(12,2),
              nullif(total_txn_count,'')::int,
              nullif(income_txn_count,'')::int,
              nullif(outcome_txn_count,'')::int,
              nullif(btc_close,'')::numeric(14,4),
              nullif(btc_pct_change_1d,'')::double precision,
              nullif(btc_7d_roc,'')::double precision,
              nullif(btc_7d_ma,'')::numeric(14,4),
              nullif(btc_volatility_7d,'')::double precision,
              case lower(coalesce(is_weekend,'')) when 'true' then true when '1' then true else false end,
              case lower(coalesce(payday_flag,'')) when 'true' then true when '1' then true else false end
            from {REDSHIFT_SCHEMA}.stg_load
            where not exists (
              select 1 from {REDSHIFT_SCHEMA}.atm_daily d
              where d.date = try_cast({REDSHIFT_SCHEMA}.stg_load.date as date)
                and d.atm_id = {REDSHIFT_SCHEMA}.stg_load.atm_id
            );"""
    ]

    for sql in statements:
        resp = client.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DATABASE,
            SecretArn=REDSHIFT_SECRET_ARN,
            Sql=sql,
        )
        desc = client.describe_statement(Id=resp["Id"])
        while desc["Status"] in ("SUBMITTED","PICKED","STARTED"):
            desc = client.describe_statement(Id=resp["Id"])
        if desc["Status"] != "FINISHED":
            die(f"Data API failed ({desc['Status']}): {desc.get('Error', '')}")
    print("[ok] Redshift COPY+INSERT completed.")

def main():
    args = parse_args()

    if not S3_BUCKET or not REDSHIFT_IAM_ROLE_ARN:
        die("Missing S3_BUCKET or REDSHIFT_IAM_ROLE_ARN in .env")

    # Decide simulation date
    if args.date:
        sim_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    elif args.next:
        if not REDSHIFT_SECRET_ARN:
            die("--next requires REDSHIFT_SECRET_ARN to query Redshift for max(date).")
        sim_date = choose_next_date_from_redshift()
    else:
        sim_date = choose_sim_date_from_local()

    print(f"[info] Simulation date: {sim_date}")

    # Build daily file, upload, then load
    local_path = slice_day(sim_date)
    s3_key = upload_to_s3(local_path, S3_BUCKET, S3_LOADS_PREFIX)
    run_redshift_copy_insert(s3_key)

if __name__ == "__main__":
    main()
