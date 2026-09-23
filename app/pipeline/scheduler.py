# ============================================================
# SNOWGUARD AUTOMATIC MONITORING SCHEDULER
# ============================================================

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

from app.pipeline.run_and_history import main as run_pipeline


# ------------------------------------------------------------
# PROJECT PATHS
# ------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "scheduler.log"


# ------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------

logger = logging.getLogger("SnowGuardScheduler")
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(
        LOG_FILE,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


# ------------------------------------------------------------
# MONITORING EXECUTION
# ------------------------------------------------------------

def execute_monitoring() -> bool:
    """Execute one complete SnowGuard monitoring run."""

    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("SNOWGUARD AUTOMATED MONITORING STARTED")
    logger.info(
        "START TIME: %s",
        start_time.strftime("%Y-%m-%d %H:%M:%S"),
    )
    logger.info("=" * 70)

    try:
        # run_and_history.main() executes all 7 stages
        # and saves the monitoring history snapshot.
        run_pipeline()

        finished_time = datetime.now()
        duration = (finished_time - start_time).total_seconds()

        logger.info("=" * 70)
        logger.info("SNOWGUARD AUTOMATED MONITORING COMPLETED")
        logger.info("STEPS: 7/7")
        logger.info("DURATION: %.2f seconds", duration)
        logger.info("PIPELINE + HISTORY COMPLETED")
        logger.info("=" * 70)

        return True

    except KeyboardInterrupt:
        raise

    except Exception as error:
        finished_time = datetime.now()
        duration = (finished_time - start_time).total_seconds()

        logger.exception("SNOWGUARD AUTOMATED MONITORING FAILED")
        logger.error("ERROR: %s", error)
        logger.error(
            "DURATION BEFORE FAILURE: %.2f seconds",
            duration,
        )
        logger.info("=" * 70)

        return False


# ------------------------------------------------------------
# SCHEDULER LOOP
# ------------------------------------------------------------

def scheduler_loop(
    interval_minutes: float,
    run_immediately: bool = True,
) -> None:
    """Run monitoring repeatedly at the requested interval."""

    if interval_minutes <= 0:
        raise ValueError(
            "Interval must be greater than 0 minutes."
        )

    interval_seconds = interval_minutes * 60

    logger.info("SNOWGUARD SCHEDULER STARTED")
    logger.info(
        "INTERVAL: %.2f minute(s)",
        interval_minutes,
    )
    logger.info("LOG FILE: %s", LOG_FILE)

    if run_immediately:
        execute_monitoring()

    while True:
        next_run = datetime.now().timestamp() + interval_seconds
        next_run_dt = datetime.fromtimestamp(next_run)

        logger.info(
            "NEXT MONITORING RUN: %s",
            next_run_dt.strftime("%Y-%m-%d %H:%M:%S"),
        )

        try:
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("SNOWGUARD SCHEDULER STOPPED BY USER")
            raise

        execute_monitoring()


# ------------------------------------------------------------
# COMMAND-LINE INTERFACE
# ------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=
        "Run SnowGuard monitoring automatically at fixed intervals."
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=60.0,
        help="Monitoring interval in minutes. Default: 60",
    )

    parser.add_argument(
        "--no-immediate-run",
        action="store_true",
        help=
        "Wait for the first interval before running the pipeline."
    )

    return parser.parse_args()


# ------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------

if __name__ == "__main__":
    args = parse_arguments()

    try:
        scheduler_loop(
            interval_minutes=args.interval,
            run_immediately=not args.no_immediate_run,
        )

    except KeyboardInterrupt:
        print("\\nSnowGuard scheduler stopped.")
