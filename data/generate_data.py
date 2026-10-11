#!/usr/bin/env python3
"""
Synthetic dataset generator for the Databricks Data Engineer Associate
Practitioner's Companion exercises.

Design goals
------------
* Independent: standard library only for CSV / NDJSON. No network access required.
  (Parquet is written only if `pyarrow` happens to be importable; otherwise skipped.)
* Controlled: you dictate schema, volume, batch count, and "dirtiness" via flags.
* Reproducible: a fixed --seed produces identical data every run.
* Portable: --outdir can be a local path OR a Unity Catalog Volume path such as
  /Volumes/<catalog>/<schema>/<volume>/raw

What it produces (a small retail star schema, exam-relevant)
------------------------------------------------------------
  customers/          dimension           (one snapshot file)
  products/           dimension           (one snapshot file)
  sales/              fact, batched       (one file per day -> Auto Loader drops)
  customers_cdc/      change feed         (INSERT/UPDATE/DELETE for MERGE / AUTO CDC)
  order_events.ndjson nested JSON         (arrays + nested objects -> explode/flatten)
  sales_dirty/        dirty variant       (nulls, dupes, outliers -> cleaning/quality)

Usage
-----
  python generate_data.py --outdir ./output
  python generate_data.py --outdir ./output --days 14 --rows-per-day 200 --seed 42
  python generate_data.py --outdir /Volumes/workspace/de_cert/raw --format ndjson --dirty

Run `python generate_data.py --help` for all flags.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
from datetime import date, datetime, timedelta

# ---------------------------------------------------------------------------
# Reference vocabularies (kept tiny and self-contained)
# ---------------------------------------------------------------------------

FIRST_NAMES = ["Ada", "Ben", "Cai", "Dev", "Eli", "Fay", "Gia", "Hana",
               "Ivo", "Jae", "Kit", "Lea", "Mira", "Nia", "Omar", "Pia"]
LAST_NAMES = ["Ash", "Bloom", "Cortez", "Dane", "Ellis", "Frost", "Guo",
              "Hart", "Imani", "Jules", "Kerr", "Lund", "Moss", "Nair"]
CITIES = [("Reston", "VA"), ("Austin", "TX"), ("Denver", "CO"), ("Reno", "NV"),
          ("Tampa", "FL"), ("Boise", "ID"), ("Akron", "OH"), ("Provo", "UT")]
SEGMENTS = ["consumer", "small_business", "enterprise"]
CATEGORIES = ["widgets", "gadgets", "sprockets", "cogs", "gizmos"]


# ---------------------------------------------------------------------------
# Dimension + fact generation
# ---------------------------------------------------------------------------

def gen_customers(rng: random.Random, n: int) -> list[dict]:
    rows = []
    start = date(2023, 1, 1)
    for i in range(1, n + 1):
        city, state = rng.choice(CITIES)
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        signup = start + timedelta(days=rng.randint(0, 700))
        rows.append({
            "customer_id": i,
            "name": f"{first} {last}",
            "email": f"{first}.{last}{i}@example.com".lower(),
            "city": city,
            "state": state,
            "segment": rng.choice(SEGMENTS),
            "signup_date": signup.isoformat(),
        })
    return rows


def gen_products(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        cat = rng.choice(CATEGORIES)
        rows.append({
            "product_id": i,
            "name": f"{cat[:-1].title()} Model {i:03d}",
            "category": cat,
            "unit_price": round(rng.uniform(4.99, 199.99), 2),
        })
    return rows


def gen_sales_for_day(rng: random.Random, day: date, rows: int,
                      n_customers: int, products: list[dict],
                      seq_start: int) -> list[dict]:
    out = []
    seq = seq_start
    for _ in range(rows):
        prod = rng.choice(products)
        qty = rng.randint(1, 8)
        ts = datetime(day.year, day.month, day.day,
                      rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))
        out.append({
            "transaction_id": seq,
            "transaction_time": ts.isoformat(sep=" "),
            "customer_id": rng.randint(1, n_customers),
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": prod["unit_price"],
            "amount": round(qty * prod["unit_price"], 2),
        })
        seq += 1
    return out


# ---------------------------------------------------------------------------
# CDC change feed (for MERGE INTO / AUTO CDC SCD exercises)
# ---------------------------------------------------------------------------

def gen_customer_cdc(rng: random.Random, customers: list[dict],
                     n_changes: int) -> list[dict]:
    """Emit a sequence of INSERT/UPDATE/DELETE operations against customers."""
    feed = []
    seq = 1
    base_time = datetime(2024, 2, 1, 8, 0, 0)
    known_ids = [c["customer_id"] for c in customers]
    next_id = max(known_ids) + 1
    for _ in range(n_changes):
        op = rng.choices(["UPDATE", "INSERT", "DELETE"], weights=[6, 3, 1])[0]
        ts = (base_time + timedelta(minutes=seq * 7)).isoformat(sep=" ")
        if op == "INSERT":
            city, state = rng.choice(CITIES)
            first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
            rec = {"customer_id": next_id, "name": f"{first} {last}",
                   "email": f"{first}.{last}{next_id}@example.com".lower(),
                   "city": city, "state": state, "segment": rng.choice(SEGMENTS)}
            known_ids.append(next_id)
            next_id += 1
        elif op == "UPDATE":
            cid = rng.choice(known_ids)
            city, state = rng.choice(CITIES)
            rec = {"customer_id": cid, "name": None, "email": None,
                   "city": city, "state": state, "segment": rng.choice(SEGMENTS)}
        else:  # DELETE
            cid = rng.choice(known_ids)
            rec = {"customer_id": cid, "name": None, "email": None,
                   "city": None, "state": None, "segment": None}
            if cid in known_ids and len(known_ids) > 1:
                known_ids.remove(cid)
        rec["op"] = op
        rec["sequence_num"] = seq
        rec["change_time"] = ts
        feed.append(rec)
        seq += 1
    return feed


# ---------------------------------------------------------------------------
# Nested JSON events (for explode / flatten exercises)
# ---------------------------------------------------------------------------

def gen_order_events(rng: random.Random, n_events: int,
                     n_customers: int, products: list[dict]) -> list[dict]:
    events = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    for i in range(1, n_events + 1):
        n_items = rng.randint(1, 4)
        items = []
        for _ in range(n_items):
            p = rng.choice(products)
            items.append({"product_id": p["product_id"],
                          "category": p["category"],
                          "qty": rng.randint(1, 5),
                          "price": p["unit_price"]})
        city, state = rng.choice(CITIES)
        events.append({
            "event_id": f"evt-{i:06d}",
            "event_time": (base_time + timedelta(minutes=i)).isoformat(sep="T"),
            "customer_id": rng.randint(1, n_customers),
            "channel": rng.choice(["web", "mobile", "store"]),
            "address": {"city": city, "state": state},
            "items": items,
            "total": round(sum(it["qty"] * it["price"] for it in items), 2),
        })
    return events


# ---------------------------------------------------------------------------
# Dirtiness injection (for cleaning / data-quality exercises)
# ---------------------------------------------------------------------------

def dirty_sales(rng: random.Random, rows: list[dict]) -> list[dict]:
    """Return a copy of sales rows with realistic defects injected."""
    out = [dict(r) for r in rows]
    for r in out:
        roll = rng.random()
        if roll < 0.05:
            r["customer_id"] = None            # missing FK
        elif roll < 0.09:
            r["quantity"] = -abs(r["quantity"])  # negative outlier
        elif roll < 0.12:
            r["unit_price"] = "N/A"            # type violation
        elif roll < 0.15:
            r["amount"] = round(r["amount"] * 1000, 2)  # magnitude outlier
    # inject a handful of exact-duplicate rows
    for _ in range(max(1, len(out) // 50)):
        out.append(dict(rng.choice(out)))
    rng.shuffle(out)
    return out


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    fields = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_ndjson(path: str, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def write_json_array(path: str, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def maybe_write_parquet(path: str, rows: list[dict]) -> bool:
    try:
        import pyarrow as pa           # noqa: WPS433 (optional dependency)
        import pyarrow.parquet as pq
    except Exception:
        return False
    if not rows:
        return False
    cols = {k: [r.get(k) for r in rows] for k in rows[0]}
    pq.write_table(pa.table(cols), path)
    return True


def write_dataset(outdir: str, name: str, rows: list[dict], fmt: str) -> str:
    d = os.path.join(outdir, name)
    os.makedirs(d, exist_ok=True)
    if fmt == "csv":
        p = os.path.join(d, f"{name}.csv"); write_csv(p, rows)
    elif fmt == "json":
        p = os.path.join(d, f"{name}.json"); write_json_array(p, rows)
    elif fmt == "parquet":
        p = os.path.join(d, f"{name}.parquet")
        if not maybe_write_parquet(p, rows):
            p = os.path.join(d, f"{name}.ndjson"); write_ndjson(p, rows)
            print(f"  ! pyarrow not available; wrote {name} as NDJSON instead")
    else:  # ndjson (default; best for streaming / Auto Loader)
        p = os.path.join(d, f"{name}.ndjson"); write_ndjson(p, rows)
    return p


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--outdir", default="./output",
                    help="Local path or UC Volume path for generated data")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed (reproducibility)")
    ap.add_argument("--customers", type=int, default=200)
    ap.add_argument("--products", type=int, default=50)
    ap.add_argument("--days", type=int, default=14,
                    help="Number of daily sales batch files (Auto Loader drops)")
    ap.add_argument("--rows-per-day", type=int, default=200)
    ap.add_argument("--cdc-changes", type=int, default=120)
    ap.add_argument("--order-events", type=int, default=300)
    ap.add_argument("--start-date", default="2024-01-01")
    ap.add_argument("--format", choices=["ndjson", "csv", "json", "parquet"],
                    default="ndjson", help="Output format for tabular datasets")
    ap.add_argument("--dirty", action="store_true",
                    help="Also emit a sales_dirty/ variant with injected defects")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(args.outdir, exist_ok=True)
    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()

    print(f"Generating into: {args.outdir}  (seed={args.seed}, format={args.format})")

    customers = gen_customers(rng, args.customers)
    products = gen_products(rng, args.products)
    write_dataset(args.outdir, "customers", customers, args.format)
    write_dataset(args.outdir, "products", products, args.format)
    print(f"  customers: {len(customers)} | products: {len(products)}")

    # Sales fact — one file per day so learners can "drop" files for Auto Loader.
    sales_dir = os.path.join(args.outdir, "sales")
    os.makedirs(sales_dir, exist_ok=True)
    all_sales, seq = [], 1
    for offset in range(args.days):
        day = start + timedelta(days=offset)
        day_rows = gen_sales_for_day(rng, day, args.rows_per_day,
                                     args.customers, products, seq)
        seq += len(day_rows)
        all_sales.extend(day_rows)
        fname = f"sales_{day.isoformat()}"
        if args.format == "csv":
            write_csv(os.path.join(sales_dir, fname + ".csv"), day_rows)
        else:
            write_ndjson(os.path.join(sales_dir, fname + ".ndjson"), day_rows)
    print(f"  sales: {len(all_sales)} rows across {args.days} daily files")

    cdc = gen_customer_cdc(rng, customers, args.cdc_changes)
    write_dataset(args.outdir, "customers_cdc", cdc, "ndjson")
    print(f"  customers_cdc: {len(cdc)} change records")

    events = gen_order_events(rng, args.order_events, args.customers, products)
    write_ndjson(os.path.join(args.outdir, "order_events.ndjson"), events)
    print(f"  order_events: {len(events)} nested JSON events")

    if args.dirty:
        dsales = dirty_sales(rng, all_sales)
        write_dataset(args.outdir, "sales_dirty", dsales, args.format)
        print(f"  sales_dirty: {len(dsales)} rows (defects injected)")

    print("Done.")


if __name__ == "__main__":
    main()
