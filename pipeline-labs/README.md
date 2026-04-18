# Pipeline Labs

End-to-end medallion pipelines you build over 2-3 hours. Each lab ships with a realistic business scenario, a synthetic data generator, and numbered notebooks you work through in order (ingestion -> bronze -> silver -> gold).

Same structure you'd ship in production - just scoped so you can finish it in an afternoon.

## Labs

| Lab | What You Build | Focus |
|---|---|---|
| [Apparel Retail 360 (DLT)](apparel-streaming/) | End-to-end retail analytics pipeline on Delta Live Tables with a full medallion architecture. | DLT, Medallion, SCD Type 2, Streaming, Data Quality Expectations |
| [Fintech Transaction Monitoring](fintech-monitoring/) | Real-time fraud-monitoring pipeline for a payment processor handling 500K+ transactions/day. | Structured Streaming, Rescued Data, Watermarked Dedup, Stream-Static Joins, Liquid Clustering |
| [DE Associate Certification Prep](de-associate-cert-prep/) | Production-grade pipeline covering every exam domain of the Databricks Data Engineer Associate cert. | Auto Loader, COPY INTO, Medallion, SCD2, Jobs, Unity Catalog |
| [PySpark Developer Cert Prep](pyspark-cert-zenith/) | E-commerce analytics pipeline covering every domain of the Spark Developer Associate cert. | DataFrame API, Structured Streaming, Data Skew, Performance Tuning |

## Folder layout

Each lab is self-contained:

```
{lab}/
  00_Setup_Environment.py        # Run once - creates catalog/schemas and seeds data
  01_*.py, 02_*.py, ...          # Numbered notebooks, work through in order
  data_generator.py              # Synthetic data (no external services needed)
  README.md                      # Business scenario + walkthrough
```

## Which one to start with?

- **Comfortable with batch, new to streaming?** [Apparel DLT](apparel-streaming/), then [Fintech Monitoring](fintech-monitoring/).
- **Preparing for the DE Associate cert?** [DE Associate Cert Prep](de-associate-cert-prep/).
- **Preparing for the Spark Developer Associate cert?** [PySpark Cert Zenith](pyspark-cert-zenith/).
- **New to Databricks entirely?** [DE Associate Cert Prep](de-associate-cert-prep/) - broadest fundamentals.

## How to run

1. Open the lab folder you want.
2. Read its `README.md` for the full business scenario and walkthrough.
3. Run `00_Setup_Environment.py` once - creates the catalog, schemas, and seeds data.
4. Work through the numbered notebooks in order.

All on Databricks Free Edition. Serverless compute, Unity Catalog, Delta Lake. No cloud account, no cluster config.
