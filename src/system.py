import pandas as pd
import logging
from typing import List, Dict, Optional
from tqdm import tqdm
from .rules import AlertRule

logger = logging.getLogger(__name__)
logging.captureWarnings(True)

def count_lines(filepath: str) -> int:
    try:
        with open(filepath, 'rb') as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0

class LogMonitoringSystem:
    def __init__(self, 
                 rules: List[AlertRule], 
                 column_names: List[str],
                 schema: Optional[Dict[str, str]] = None):
        self.rules = rules
        self.column_names = column_names
        self.schema = schema

    def ingest_and_analyze(self, file_path: str, chunk_size: int):
        logger.info(f"Starting analysis for: {file_path}")
        total_lines = count_lines(file_path)
        if total_lines <= 1:
            logger.error(f"File not found or is empty: {file_path}")
            return
            
        try:
            chunks = pd.read_csv(
                file_path, 
                chunksize=chunk_size,
                names=self.column_names,
                header=None,
                skiprows=[0],
                sep=',',
                on_bad_lines='warn',
                dtype=self.schema,
                low_memory=True
            )

            with tqdm(total=total_lines, desc="Processing Logs", unit=" lines") as pbar:
                for chunk in chunks:
                    pbar.update(len(chunk))
                    if 'date' not in chunk.columns:
                        logger.error("Column 'date' not found. Skipping chunk.")
                        continue

                    chunk['date'] = pd.to_datetime(chunk['date'], unit='s', errors='coerce')
                    
                    invalid_dates = chunk['date'].isna().sum()
                    if invalid_dates > 0:
                        logger.debug(f"Found {invalid_dates} invalid timestamps in chunk, dropping them.")
                    
                    chunk.dropna(subset=['date'], inplace=True)
                    
                    if not chunk.empty:
                        self._process_chunk(chunk)
        
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
        except pd.errors.ParserError as e:
            logger.error(f"CSV parsing error: {e}")
        except Exception:
            logger.exception("An unexpected error occurred during CSV processing")

    def _process_chunk(self, df: pd.DataFrame):
        for rule in self.rules:
            alerts = rule.check(df)
            if not alerts.empty:
                self._send_alert(type(rule).__name__, alerts)

    def _send_alert(self, rule_name: str, alert_data: pd.DataFrame):
        logger.warning(f"[ALERT] Rule: {rule_name} triggered for {len(alert_data)} groups!")
        logger.warning("Alert details (first 10 rows):\n" + alert_data.head(10).to_string())