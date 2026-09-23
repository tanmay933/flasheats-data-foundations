import json
import logging
from pathlib import Path

from src.ingestion.ingest import ingest_all
from src.validation.validate import validate_all
from src.transformation.transform import transform_all
from src.metrics.metrics import calculate_metrics


OUTPUT_DIR = Path("outputs")
LOG_FILE = OUTPUT_DIR / "pipeline.log"
METRICS_FILE = OUTPUT_DIR / "metrics_report.json"


def setup_logging():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] pipeline: %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="w"),
            logging.StreamHandler()
        ]
    )


def write_metrics_report(report):
    with open(METRICS_FILE, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)


def run_pipeline():
    logger = logging.getLogger(__name__)

    logger.info("Starting FlashEats delivery reliability pipeline.")

    # 1. Ingest raw sources
    logger.info("STEP 1/4: Ingestion")
    raw_data = ingest_all()

    if not raw_data:
        raise RuntimeError("Pipeline stopped: ingestion returned no data.")

    # 2. Validate raw data
    logger.info("STEP 2/4: Validation")
    validation_result = validate_all(raw_data)

    validation_summary = validation_result.get("summary", {})

    error_records = validation_summary.get("total_errors", 0)
    warning_records = validation_summary.get("total_warnings", 0)

    logger.info(
    "Validation completed with %s error-severity record(s) and %s warning-severity record(s).",
    error_records,
    warning_records
    )

    # Validation is observational here.
    # Known data-quality issues are preserved and handled explicitly
    # during transformation rather than silently discarded.

    # 3. Transform and model
    logger.info("STEP 3/4: Transformation")
    transformed_data = transform_all(raw_data)

    if not transformed_data:
        raise RuntimeError("Pipeline stopped: transformation returned no data.")

    # 4. Calculate metrics
    logger.info("STEP 4/4: Metrics")
    metrics_report = calculate_metrics(transformed_data)

    if not metrics_report:
        raise RuntimeError("Pipeline stopped: metrics calculation returned no result.")

    # Save reproducible final output
    write_metrics_report(metrics_report)

    logger.info("Final metrics written to %s", METRICS_FILE)
    logger.info("Pipeline completed successfully.")

    return metrics_report


if __name__ == "__main__":
    setup_logging()

    try:
        run_pipeline()
    except Exception as exc:
        logging.getLogger(__name__).exception(
            "Pipeline failed: %s",
            exc
        )
        raise