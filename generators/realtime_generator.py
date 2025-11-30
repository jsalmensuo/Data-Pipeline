import json
import pandas as pd
import time

def outage_stream(df, delay=0.1):
    """Yield one outage at a time with a delay"""
    for _, row in df.iterrows():
        yield row.to_dict()
        time.sleep(delay)