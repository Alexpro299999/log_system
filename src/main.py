import os
from .rules import FatalErrorRateRule, BundleFatalErrorRule
from .system import LogMonitoringSystem

def main():
    data_path = os.getenv('DATA_PATH', '/app/data/alert_project_data.csv')
    chunk_size = int(os.getenv('CHUNK_SIZE', 1000000))

    rules = [
        FatalErrorRateRule(threshold=10, time_window='1min'),
        BundleFatalErrorRule(threshold=10, time_window='1h')
    ]

    system = LogMonitoringSystem(rules)
    
    try:
        system.ingest_and_analyze(data_path, chunk_size)
    except FileNotFoundError:
        print(f"Error: File not found at {data_path}")
    except Exception as e:
        print(f"Critical System Error: {e}")

if __name__ == "__main__":
    main()