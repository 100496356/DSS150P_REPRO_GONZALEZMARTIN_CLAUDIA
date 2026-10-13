import pandas as pd
import pytest

from src.config import path_for


@pytest.fixture
def source_df():
    return pd.read_csv(path_for('source_file'))


def db_available() -> bool:
    try:
        from src.db import connect
        with connect():
            return True
    except Exception:
        return False
