# FlashEats — Class 8 Pack

Building a Simple, Dependable Data Pipeline.

![Notebook](https://img.shields.io/badge/Notebook-Jupyter-F37626?style=flat-square&logo=jupyter)
![Language](https://img.shields.io/badge/Language-Python-3776AB?style=flat-square&logo=python)
![Pipeline](https://img.shields.io/badge/Pipeline-ETL-orange?style=flat-square)
![Status](https://img.shields.io/badge/status-complete-brightgreen?style=flat-square)

> Building a Simple, Dependable Data Pipeline.

---

## Topic

Building a Simple, Dependable Data Pipeline.

---

## Recommended Class Flow

1. Open `FlashEats_Class8_Walkthrough.ipynb`
2. Walk through the production-handoff story
3. Move into `Class8_Project/`
4. Run the complete pipeline
5. Trigger controlled failures
6. Complete the Gate 2 data-readiness review

---

## Why FlashEats Is Used Again

For this class, a controlled operational dataset is more useful than a static public dataset because the pack includes:

- SQLite source data
- CSV source data
- a paginated HTTP API
- deterministic HTTP 500 and 429 failures
- the Class 7 order-journey model
- known validation rules

This allows the class to teach reliability behavior rather than only batch transformation.

---

## Expected Output

A re-runnable pipeline with:

- explicit extract / validate / clean / transform / save stages
- configuration outside core logic
- bounded retries
- raw-response preservation
- required-column, null, duplicate and freshness checks
- basic logging
- useful failure messages
- idempotent output by logical run date
- one-command execution
- Gate 2 readiness notes

---

## Not Included

No Airflow, Spark, Kafka, distributed processing, or enterprise observability.

---


## Author

**Tanmay Mittal**
Roll No.: **24BCS10491**