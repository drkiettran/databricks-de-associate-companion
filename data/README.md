# Data dictionary

All data here is **synthetic**, produced by `generate_data.py`. Nothing is
downloaded; the same `--seed` reproduces the same data byte-for-byte.

Run `python generate_data.py --help` for all flags. Default output is NDJSON
(newline-delimited JSON), which is the friendliest format for Auto Loader and
streaming reads.

## Datasets

### `customers/` — dimension (one snapshot)
| column | type | notes |
|---|---|---|
| customer_id | int | primary key |
| name | string | |
| email | string | |
| city, state | string | |
| segment | string | consumer / small_business / enterprise |
| signup_date | date (ISO string) | |

### `products/` — dimension (one snapshot)
| column | type | notes |
|---|---|---|
| product_id | int | primary key |
| name | string | |
| category | string | widgets / gadgets / sprockets / cogs / gizmos |
| unit_price | double | |

### `sales/` — fact, one file per day
`sales_YYYY-MM-DD.ndjson`, one file per day for `--days`. Drop them into a
Volume one at a time to exercise **Auto Loader** incremental ingestion.

| column | type | notes |
|---|---|---|
| transaction_id | int | monotonically increasing |
| transaction_time | timestamp (string) | |
| customer_id | int | FK → customers |
| product_id | int | FK → products |
| quantity | int | |
| unit_price | double | |
| amount | double | quantity × unit_price |

### `customers_cdc/` — change feed
An ordered stream of INSERT / UPDATE / DELETE operations against customers, for
**MERGE INTO** and **AUTO CDC / APPLY CHANGES** (SCD Type 1/2) exercises.

| column | type | notes |
|---|---|---|
| customer_id | int | key |
| name, email, city, state, segment | string | null on DELETE / partial UPDATE |
| op | string | INSERT / UPDATE / DELETE |
| sequence_num | int | ordering key for out-of-order handling |
| change_time | timestamp (string) | |

### `order_events.ndjson` — nested JSON
Events with a nested `address` object and an `items` array, for **explode /
flatten** and semi-structured-ingestion exercises.

```json
{"event_id": "...", "event_time": "...", "customer_id": 1, "channel": "web",
 "address": {"city": "...", "state": "..."},
 "items": [{"product_id": 1, "category": "...", "qty": 2, "price": 9.99}],
 "total": 19.98}
```

### `sales_dirty/` — defective variant (with `--dirty`)
A copy of the sales fact with injected defects for **cleaning / data-quality**
exercises: missing foreign keys (nulls), negative-quantity and magnitude
outliers, a type violation (`"N/A"` in a numeric column), and exact-duplicate
rows.

## Common recipes

```bash
# Default 14 daily batches, 200 customers, 50 products
python generate_data.py --outdir ./output

# Bigger volume for tuning/optimization exercises
python generate_data.py --outdir ./output --days 30 --rows-per-day 5000

# CSV instead of NDJSON
python generate_data.py --outdir ./output --format csv

# Add the dirty variant for cleaning exercises
python generate_data.py --outdir ./output --dirty

# Write directly into a Unity Catalog Volume from a notebook %sh cell
python generate_data.py --outdir /Volumes/main/companion/raw
```
