import re
import os
import json
import pandas as pd

#-------------INGEST-------------

df = pd.read_csv(
    "data/raw_data1.csv",
    dtype=str,
    keep_default_na=False,
    encoding="utf-8-sig"
)

# ----------STANDARDIZE ----------

dt = pd.to_datetime(df["transaction_date"], errors="coerce", dayfirst=True)
df["transaction_date_std"] = dt.dt.strftime("%Y-%m-%d")

def clean_amount(x):
    s = str(x)
    s2 = re.sub(r"[^0-9.\-]", "", s)
    if s2 in {"", ".", "-"}:
        return None, "not_numeric"
    try:
        return float(s2), ""
    except:
        return None, "not_numeric"

vals, errs = [], []
for v in df["amount"]:
    v2, e = clean_amount(v)
    vals.append(v2)
    errs.append(e)

df["amount_std"] = vals
df["amount_parse_error"] = errs

print("=== Standardized Preview ===")
print(df[[
    "customer_id",
    "transaction_date",
    "transaction_date_std",
    "amount",
    "amount_std",
    "amount_parse_error"
]])

os.makedirs("outputs", exist_ok=True)
df.to_csv("outputs/standardized_preview.csv", index=False, encoding="utf-8-sig")
print("Saved: outputs/standardized_preview.csv")

# ----------VALIDATION ----------

import json

with open("data/schema.json", "r", encoding="utf-8") as f:
    schema = json.load(f)

issues = []

for _, row in df.iterrows():
    row_issues = []

    # customer_id pattern
    if not re.fullmatch(schema["properties"]["customer_id"]["pattern"], str(row["customer_id"])):
        row_issues.append("pattern:customer_id")

    # invalid or missing date
    if pd.isna(row["transaction_date_std"]) or str(row["transaction_date_std"]).strip() == "":
        row_issues.append("invalid_date")

    # amount validation
    if pd.isna(row["amount_std"]):
        row_issues.append("invalid_amount")

    elif row["amount_std"] < schema["properties"]["amount"]["minimum"]:
        row_issues.append("negative_amount")

    # currency enum
    if row["currency"] not in schema["properties"]["currency"]["enum"]:
        row_issues.append("invalid_currency")

    # status enum
    if row["status"] not in schema["properties"]["status"]["enum"]:
        row_issues.append("invalid_status")

    # country missing
    if str(row["country"]).strip() == "":
        row_issues.append("missing_country")

    issues.append(";".join(row_issues))

df["dq_issues"] = issues

valid_df = df[df["dq_issues"] == ""]
invalid_df = df[df["dq_issues"] != ""]

valid_df.to_csv("outputs/cleaned_data.csv", index=False, encoding="utf-8-sig")
invalid_df.to_csv("outputs/invalid_records.csv", index=False, encoding="utf-8-sig")

print("\n=== Validation Summary ===")
print("Total rows:", len(df))
print("Valid rows:", len(valid_df))
print("Invalid rows:", len(invalid_df))
print("\nInvalid records and reasons:")
print(invalid_df[["customer_id", "dq_issues"]])
print("\nSaved: outputs/cleaned_data.csv and outputs/invalid_records.csv")

from datetime import datetime

# ----------HTML DATA QUALITY REPORT ----------
def issue_counts(series):
    counts = {}
    for cell in series.fillna(""):
        txt = str(cell).strip()
        if txt == "":
            continue
        for token in txt.split(";"):
            token = token.strip()
            if token:
                counts[token] = counts.get(token, 0) + 1
    return counts

total = len(df)
valid = len(valid_df)
invalid = len(invalid_df)
invalid_pct = (invalid / total * 100) if total else 0

counts = issue_counts(invalid_df["dq_issues"])
top_issues = sorted(counts.items(), key=lambda x: x[1], reverse=True)

top_issues_html = "<table border='1' cellpadding='6' cellspacing='0'><tr><th>Issue</th><th>Count</th></tr>"
for issue, c in top_issues:
    top_issues_html += f"<tr><td>{issue}</td><td>{c}</td></tr>"
top_issues_html += "</table>"

invalid_preview_html = invalid_df.head(20).to_html(index=False, escape=True)

report_html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Data Quality Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    h1 {{ margin-bottom: 4px; }}
    .muted {{ color: #666; margin-top: 0; }}
    .card {{ border: 1px solid #ddd; border-radius: 10px; padding: 16px; margin: 16px 0; }}
    table {{ border-collapse: collapse; }}
    th {{ background: #f3f3f3; }}
  </style>
</head>
<body>
  <h1>Data Quality Report</h1>
  <p class="muted">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

  <div class="card">
    <h2>Summary</h2>
    <ul>
      <li><b>Total rows:</b> {total}</li>
      <li><b>Valid rows:</b> {valid}</li>
      <li><b>Invalid rows:</b> {invalid}</li>
      <li><b>Invalid %:</b> {invalid_pct:.2f}%</li>
    </ul>
  </div>

  <div class="card">
    <h2>Top Issues</h2>
    {top_issues_html}
  </div>

  <div class="card">
    <h2>Invalid Records Preview (first 20)</h2>
    {invalid_preview_html}
  </div>

  <div class="card">
    <h2>Output Files</h2>
    <ul>
      <li>outputs/standardized_preview.csv</li>
      <li>outputs/cleaned_data.csv</li>
      <li>outputs/invalid_records.csv</li>
      <li>outputs/data_quality_report.html</li>
    </ul>
  </div>
</body>
</html>
"""

with open("outputs/data_quality_report.html", "w", encoding="utf-8") as f:
    f.write(report_html)

print("\nSaved: outputs/data_quality_report.html")

