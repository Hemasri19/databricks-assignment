import logging
import os
from logging.handlers import RotatingFileHandler


def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a configured logger instance.

    Args:
        name (str): Name of the logger (usually __name__)

    Returns:
        logging.Logger: Configured logger
    """

    logger = logging.getLogger(name)

    # Prevent duplicate logs in Airflow
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # ----------------------------
    # Log Format
    # ----------------------------
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ----------------------------
    # Console Handler
    # ----------------------------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ----------------------------
    # File Handler (Optional)
    # ----------------------------
    log_dir = "/usr/local/airflow/logs/custom"

    try:
        os.makedirs(log_dir, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=f"{log_dir}/application.log",
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3,
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    except Exception:
        # If file logging fails, Airflow default logging still works
        pass

    logger.propagate = False

    return logger