# ============================================================
# NimbusAI — Clean Data for Power BI Dashboard
# Input:  nimbus_master_dataset.csv
# Output: nimbus_dashboard_clean.csv
# Run:    python clean_for_dashboard.py
# ============================================================

import pandas as pd
import numpy as np

# ── Load ─────────────────────────────────────────────────────
INPUT_FILE  = "E:/Placements/Roado/Take-Home Challenge/Project/nimbus_master_dataset.csv"   # change path if needed
OUTPUT_FILE = "nimbus_dashboard_clean.csv"

df = pd.read_csv(INPUT_FILE)

print("=" * 50)
print("BEFORE CLEANING")
print("=" * 50)
print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
print("Nulls per column:")
print(df.isnull().sum().to_string())

# ============================================================
# 1. Text / Categorical columns → fill with descriptive string
# ============================================================

df["company_name"]        = df["company_name"].fillna("Unknown")
df["industry"]            = df["industry"].fillna("Unknown")
df["company_size"]        = df["company_size"].fillna("Unknown")
df["country_code"]        = df["country_code"].fillna("Unknown")
df["country_name"]        = df["country_name"].fillna("Unknown")
df["timezone"]            = df["timezone"].fillna("Unknown")
df["contact_email"]       = df["contact_email"].fillna("Unknown")
df["plan_tier"]           = df["plan_tier"].fillna("Unknown")
df["plan_name"]           = df["plan_name"].fillna("Unknown")
df["status"]              = df["status"].fillna("Unknown")
df["billing_cycle"]       = df["billing_cycle"].fillna("Unknown")
df["cancellation_reason"] = df["cancellation_reason"].fillna("Not Cancelled")
df["churn_reason"]        = df["churn_reason"].fillna("Not Churned")

# ============================================================
# 2. Date columns → fill with empty string
#    (Power BI treats blank strings as empty dates cleanly)
# ============================================================

df["churned_at"] = df["churned_at"].fillna("0")
df["end_date"]   = df["end_date"].fillna("0")
df["start_date"] = df["start_date"].fillna("")

# ============================================================
# 3. Numeric columns
#    mrr_usd        → 0 (no subscription = no revenue)
#    nps_score      → median (neutral imputation)
#    avg_satisfaction → 0 (no tickets = no satisfaction score)
# ============================================================

df["mrr_usd"]          = df["mrr_usd"].fillna(0)
df["nps_score"]        = df["nps_score"].fillna(round(df["nps_score"].median(), 1))
df["avg_satisfaction"] = df["avg_satisfaction"].fillna(0)

# ============================================================
# 4. Boolean columns → convert True/False to 1/0
#    Power BI DAX works reliably with integers, not booleans
# ============================================================

df["is_churned"]  = df["is_churned"].map({True: 1, False: 0})
df["is_active"]   = df["is_active"].map({True: 1, False: 0})
df["high_ticket"] = df["high_ticket"].map({True: 1, False: 0})

# ============================================================
# 5. Ticket bucket — ensure correct sort order for Power BI
#    Add a numeric sort column so charts display in right order
# ============================================================

bucket_order = {
    "0 tickets":   1,
    "1 ticket":    2,
    "2-3 tickets": 3,
    "4-6 tickets": 4,
    "7+ tickets":  5
}
df["ticket_bucket_sort"] = df["ticket_bucket"].map(bucket_order).fillna(99).astype(int)

# ============================================================
# 6. Verify — should be zero nulls everywhere
# ============================================================

print("\n" + "=" * 50)
print("AFTER CLEANING")
print("=" * 50)
print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
print("Nulls remaining:")
remaining = df.isnull().sum()
print(remaining.to_string())

if remaining.sum() == 0:
    print("\n✓ Zero nulls — file is ready for Power BI")
else:
    print("\n⚠ Some nulls remain — check above columns")

# ============================================================
# 7. Save
# ============================================================

df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✓ Saved: {OUTPUT_FILE}")
print(f"  Rows: {len(df)}")
print(f"  Columns: {len(df.columns)}")

print("\n" + "=" * 50)
print("COLUMN SUMMARY FOR POWER BI")
print("=" * 50)
print(f"{'Column':<25} {'Type':<15} {'Unique Values / Range'}")
print("-" * 70)
for col in df.columns:
    dtype = str(df[col].dtype)
    if dtype in ["int64", "float64"]:
        info = f"{df[col].min():.2f} to {df[col].max():.2f}"
    else:
        n_unique = df[col].nunique()
        info = f"{n_unique} unique values"
    print(f"{col:<25} {dtype:<15} {info}")
