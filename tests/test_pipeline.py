import pytest
from unittest.mock import patch, MagicMock
from src.etl_process import ETLPipeline

@patch('pandas.read_sql')
def test_extract_retries_on_failure(mock_read):
    # Simulamos error de conexión
    mock_read.side_effect = Exception("Network Failure")
    
    pipeline = ETLPipeline()
    with pytest.raises(Exception):
        pipeline.extract("2026-01-01")
    
    # Verificamos que se intentó 3 veces (según la config del decorador)
    assert mock_read.call_count == 3