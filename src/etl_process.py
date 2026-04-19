import pandas as pd
from sqlalchemy import create_engine
from tenacity import retry, stop_after_attempt, wait_exponential
from pydantic import ValidationError
from .models import TransactionSchema
from .config import settings

class ETLPipeline:
    def __init__(self):
        self.source_engine = create_engine(settings.SOURCE_DB_URL, pool_size=5)
        self.target_engine = create_engine(settings.TARGET_DB_URL, pool_size=10, max_overflow=20)

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
    def extract(self, last_timestamp):
        query = f"SELECT * FROM legacy_transactions WHERE updated_at > '{last_timestamp}'"
        # Chunking para manejar memoria
        return pd.read_sql(query, self.source_engine, chunksize=5000)

    def validate(self, df_chunk):
        validated = []
        for _, row in df_chunk.iterrows():
            try:
                validated.append(TransactionSchema(**row.to_dict()).dict())
            except ValidationError as e:
                print(f"Skipping malformed row: {e}")
        return pd.DataFrame(validated)

    def load(self, df):
        if not df.empty:
            df.to_sql('transactions', self.target_engine, if_exists='append', index=False)

    def run(self, last_timestamp):
        chunks = self.extract(last_timestamp)
        for chunk in chunks:
            clean_df = self.validate(chunk)
            self.load(clean_df)