import os.path
from pathlib import Path

from dotenv import load_dotenv

DB_URI_STR = 'HHS_DB_URI'
DB_NAME_STR = 'HHS_DATABASE_NAME'
EXTRACTOR_THEME_STR = 'HHS_EXTRACTOR_KAFKA_THEME'
PROCESSING_THEME_STR = 'HHS_PROCESSING_KAFKA_THEME'
KAFKA_PORT_STR = 'HHS_KAFKA_PORT'

dotenv_path = Path('../../.hhvacenv')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)