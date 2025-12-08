from abc import ABC, abstractmethod
import pandas as pd

class AlertRule(ABC):
    @abstractmethod
    def check(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

class FatalErrorRateRule(AlertRule):
    def __init__(self, threshold: int, time_window: str):
        self.threshold = threshold
        self.time_window = time_window

    def check(self, df: pd.DataFrame) -> pd.DataFrame:
        fatal_errors = df[df['severity'] == 'Error']
        if fatal_errors.empty:
            return pd.DataFrame()
        
        resampled = fatal_errors.set_index('date').resample(self.time_window).size()
        alerts = resampled[resampled > self.threshold]
        
        return alerts.to_frame(name='error_count')

class BundleFatalErrorRule(AlertRule):
    def __init__(self, threshold: int, time_window: str):
        self.threshold = threshold
        self.time_window = time_window

    def check(self, df: pd.DataFrame) -> pd.DataFrame:
        fatal_errors = df[df['severity'] == 'Error']
        if fatal_errors.empty:
            return pd.DataFrame()

        grouped = fatal_errors.groupby(['bundle_id', pd.Grouper(key='date', freq=self.time_window)]).size()
        alerts = grouped[grouped > self.threshold]
        
        return alerts.to_frame(name='error_count')