import logging
import yaml
from typing import Dict, Any
from datetime import datetime
import pandas as pd

def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """Configure logging with settings from config.yaml."""
    logging.basicConfig(
        level=config['logging']['level'],
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=config['logging']['file']
    )
    return logging.getLogger(__name__)

def load_config(file_path: str = 'config.yaml') -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def validate_data(df: pd.DataFrame, rules: Dict[str, Dict[str, Any]]) -> list:
    """Validate DataFrame against predefined rules."""
    issues = []
    for column, rule in rules.items():
        if rule['type'] == 'not_null' and df[column].isna().sum() > 0:
            issues.append(f"{column}: {df[column].isna().sum()} null values")
        elif rule['type'] == 'range':
            out_of_range = df[column].apply(lambda x: x < rule['min'] or x > rule['max']).sum()
            if out_of_range > 0:
                issues.append(f"{column}: {out_of_range} values out of range [{rule['min']}, {rule['max']}]")
    return issues