-- 1) Create a schema for this project
create schema if not exists atm;

-- 2) Create a staging table that accepts everything as text (robust for CSV COPY)
drop table if exists atm.stg_daily;
create table atm.stg_daily (
  date              varchar(32),
  atm_id            varchar(64),
  atm_city          varchar(128),
  withdrawals_usd   varchar(64),
  total_txn_count   varchar(64),
  income_txn_count  varchar(64),
  outcome_txn_count varchar(64),
  btc_close         varchar(64),
  btc_pct_change_1d varchar(64),
  btc_7d_roc        varchar(64),
  btc_7d_ma         varchar(64),
  btc_volatility_7d varchar(64),
  is_weekend        varchar(16),
  payday_flag       varchar(16)
);

-- 3) Create the typed target table for historical+incremental data
drop table if exists atm.atm_daily;
create table atm.atm_daily (
  date              date encode zstd,
  atm_id            varchar(64) encode zstd,
  atm_city          varchar(128) encode zstd,
  withdrawals_usd   numeric(12,2) encode zstd,
  total_txn_count   int encode zstd,
  income_txn_count  int encode zstd,
  outcome_txn_count int encode zstd,
  btc_close         numeric(14,4) encode zstd,
  btc_pct_change_1d double precision encode zstd,
  btc_7d_roc        double precision encode zstd,
  btc_7d_ma         numeric(14,4) encode zstd,
  btc_volatility_7d double precision encode zstd,
  is_weekend        boolean encode zstd,
  payday_flag       boolean encode zstd
);

-- 4) A second staging table just for the daily loads path
drop table if exists atm.stg_load;
create table atm.stg_load (like atm.stg_daily);