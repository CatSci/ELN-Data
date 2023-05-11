import os

env_file = '.env'

SCHEMA_FILE_PATH = os.path.join('elndata','config', 'schema.yaml')
DATA_DIR: str = 'data'
YAML_DATA: str = 'data.yaml'
CSV_DATA: str = 'data.csv'

# artifact dir to save data with timestamp
ARTIFACT_DIR: str = 'artifact'
# data dir where data will be saved with timestamp
DATA_EXTRACTION_DIR: str = 'data'
# data dir for final data
DATA_DIR: str = 'data_extracted'

# Final data

