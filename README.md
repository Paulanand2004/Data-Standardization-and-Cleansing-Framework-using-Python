# Data-Standardization-and-Cleansing-Framework-using-Python

## Project Overview
This project implements a Python-based data standardization and cleansing framework designed to handle real-world data quality issues in CSV datasets. The framework ingests raw transactional data, standardizes inconsistent date and numeric formats, validates records against a configurable JSON schema, flags invalid records with detailed reasons, and generates a comprehensive data quality report.

The solution reflects common data engineering and data governance practices used in ETL pipelines and analytics workflows.

---

## Key Objectives
- Ingest raw CSV data containing inconsistencies and errors
- Standardize date formats into ISO format (`YYYY-MM-DD`)
- Clean and normalize numeric fields with currency symbols and text
- Validate records using schema-based rules
- Separate valid and invalid records
- Generate a clear, stakeholder-friendly data quality report

---

## Technologies Used
- Python
- pandas
- JSON Schema
- Regular Expressions (re)
- HTML (for reporting)

---

## Project Structure

Data-Standardization-and-Cleansing-Framework-using-Python/
├── data/
│ ├── raw_data1.csv # Raw input dataset
│ └── schema.json # Validation rules
├── outputs/
│ ├── standardized_preview.csv
│ ├── cleaned_data.csv
│ ├── invalid_records.csv
│ └── data_quality_report.html
├── run_demo.py # Main execution script
└── README.md


---

## Input Dataset
The raw dataset intentionally contains real-world data quality issues such as:
- Mixed date formats (`YYYY-MM-DD`, `DD/MM/YYYY`, `YYYY/MM/DD`)
- Invalid dates (e.g., month = 13)
- Currency symbols and commas in numeric values
- Negative and non-numeric amounts
- Missing mandatory fields
- Invalid enum values (currency and status)

---

## Data Standardization
The framework standardizes:
- **Dates** → ISO format (`YYYY-MM-DD`)
- **Amounts** → Numeric float values
- Raw values are preserved, and standardized values are stored in separate columns for auditability.

---

## Data Validation
Validation rules are defined in `schema.json` and include:
- Customer ID pattern enforcement
- Required field checks
- Minimum value constraints for numeric fields
- Enum validation for currency and transaction status
- Missing field detection

Each invalid record is tagged with detailed issue reasons in a `dq_issues` column.

---

## Outputs Generated
After execution, the following outputs are produced:

| File | Description |
|-----|------------|
| `standardized_preview.csv` | Raw data with standardized columns |
| `cleaned_data.csv` | Fully valid records only |
| `invalid_records.csv` | Invalid records with issue reasons |
| `data_quality_report.html` | Interactive data quality summary report |

---

## How to Run the Project

### Install dependencies
```bash
python -m pip install pandas

RUN PIPELINE : python run_demo.py


