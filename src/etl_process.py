import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from pydantic import ValidationError
from src.models import TransactionSchema

class ETLPipeline:

    def __init__(self, source_engine, target_engine):
        """
        Inyección de dependencias:
        """
        self.source_engine = source_engine
        self.target_engine = target_engine

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
    def extract(self, last_timestamp):
        query = f"SELECT * FROM transactions WHERE updated_at > '{last_timestamp}'"
        return pd.read_sql(query, self.source_engine, chunksize=5000)

    def validate(self, df_chunk):
        validated = []
        for _, row in df_chunk.iterrows():
            try:
                # Validamos cada fila
                validated.append(TransactionSchema(**row.to_dict()).dict())
            except ValidationError as e:
                print(f"Error de validación: {e}")
        return pd.DataFrame(validated)

    def load(self, df):
        if not df.empty:
            # Carga por lotes
            df.to_sql('transactions', self.target_engine, if_exists='append', index=False)

    def run(self, last_timestamp):
        chunks = self.extract(last_timestamp)
        for chunk in chunks:
            clean_df = self.validate(chunk)
            self.load(clean_df)