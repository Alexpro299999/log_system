import os
import logging
from .rules import FatalErrorRateRule, BundleFatalErrorRule
from .system import LogMonitoringSystem
from .config import COLUMN_NAMES, SCHEMA

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
    )

    data_path = os.getenv('DATA_PATH', 'data/alert_project_data.csv')
    chunk_size = int(os.getenv('CHUNK_SIZE', 1000000))

    rules = [
        FatalErrorRateRule(threshold=10, time_window='1min'),
        BundleFatalErrorRule(threshold=10, time_window='1h')
    ]

    system = LogMonitoringSystem(rules, COLUMN_NAMES, SCHEMA)
    system.ingest_and_analyze(data_path, chunk_size)

if __name__ == "__main__":
    main()