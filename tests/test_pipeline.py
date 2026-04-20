import pytest
from unittest.mock import patch, MagicMock
from src.etl_process import ETLPipeline

@patch('pandas.read_sql')
def test_extract_retries_on_failure(mock_read):
    mock_source = MagicMock()
    mock_target = MagicMock()

    # Simulamos error de conexión
    mock_read.side_effect = Exception("Network Failure")
    
    pipeline = ETLPipeline(mock_source, mock_target)
    with pytest.raises(Exception):
        pipeline.extract("2026-01-01")
    
    # Verificamos que se intentó 3 veces (según la config del decorador)
    assert mock_read.call_count == 3

def test_extract_retries_on_successful():
    # Configuramos el mock para que devuelva un objeto falso en lugar de una conexión real
    mock_source = MagicMock()
    mock_target = MagicMock()
    
    # Ahora, al instanciar, no se intentará conectar a ninguna base de datos
    pipeline = ETLPipeline(mock_source, mock_target)
    
    # Verificamos que se haya llamado a create_engine dos veces (source y target)
