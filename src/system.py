import pandas as pd
import warnings
from typing import List
from .rules import AlertRule

warnings.filterwarnings("ignore")

class LogMonitoringSystem:
    def __init__(self, rules: List[AlertRule]):
        self.rules = rules
        self.column_names = [
            'error_code', 'error_message', 'severity', 'log_location', 'mode', 
            'model', 'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 
            'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 
            'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 
            'country_code', 'date'
        ]

    def ingest_and_analyze(self, file_path: str, chunk_size: int):
        print(f"Starting analysis for: {file_path}", flush=True)
        
        try:
            chunks = pd.read_csv(
                file_path, 
                chunksize=chunk_size,
                names=self.column_names,
                header=0,
                sep=',',
                on_bad_lines='skip',
                dtype={'date': float},
                low_memory=False
            )

            total_processed = 0

            for chunk in chunks:
                chunk['date'] = pd.to_datetime(chunk['date'], unit='s')
                
                self._process_chunk(chunk)
                
                total_processed += len(chunk)
                print(f"Processed {total_processed} records...", flush=True)
                
        except Exception as e:
             print(f"Error reading CSV: {e}", flush=True)
             raise e

    def _process_chunk(self, df: pd.DataFrame):
        for rule in self.rules:
            alerts = rule.check(df)
            if not alerts.empty:
                self._send_alert(type(rule).__name__, alerts)

    def _send_alert(self, rule_name: str, alert_data: pd.DataFrame):
        print(f"\n[ALERT] Rule: {rule_name} triggered!", flush=True)
        print(alert_data, flush=True)
        print("-" * 30, flush=True)