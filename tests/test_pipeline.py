import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.etl_process import ETLPipeline


@pytest.fixture
def pipeline():
    mock_source = MagicMock()
    mock_target = MagicMock()
    return ETLPipeline(source_engine=mock_source, target_engine=mock_target)

def test_run_success(pipeline):
    """Test 1: Happy Path - Los datos se extraen y cargan correctamente."""
    # Simulamos que la extracción devuelve un chunk con datos
    df = pd.DataFrame({
        'transaction_id': ['1'], 
        'source_system_id': ['src1'],
        'amount': [100.0], 
        'currency': ['USD'], 
        'transaction_date': ['2026-04-01 10:00:00']
    })
    pipeline.extract = MagicMock(return_value=[df])
    pipeline.load = MagicMock()

    pipeline.run("2026-01-01")

    assert pipeline.load.called
    assert pipeline.load.call_args[0][0].shape[0] == 1

def test_run_empty_source(pipeline):
    """Test 2: Caso de borde - No hay registros nuevos en el sistema legado."""
    # Simulamos que el origen no devuelve nada
    pipeline.extract = MagicMock(return_value=[pd.DataFrame()])
    pipeline.load = MagicMock()

    pipeline.run("2026-01-01")

    # Load no debería llamarse con un DataFrame vacío o debería manejarlo
    assert pipeline.load.called == False

def test_validation_logic(pipeline):
    """Test 3: Fila 1: Correcta, Fila 2: Monto negativo"""
    df = pd.DataFrame([
        {'transaction_id': '1', 'source_system_id': 'src1', 'amount': 100.0, 'currency': 'USD', 'transaction_date': '2026-04-01 10:00:00'},
        {'transaction_id': '2', 'source_system_id': 'src1', 'amount': -50.0, 'currency': 'USD', 'transaction_date': '2026-04-01 10:00:00'}
    ])
    clean_df = pipeline.validate(df)
    # Ahora sí debería pasar
    assert len(clean_df) == 1
    assert clean_df.iloc[0]['transaction_id'] == '1'

@patch('src.etl_process.pd.read_sql')
def test_retry_mechanism(mock_read_sql, pipeline):
    """Test 4: Resiliencia - Verificar que el sistema reintenta ante fallos."""
    # Simulamos error de conexión
    mock_read_sql.side_effect = Exception("Database Down")

    with pytest.raises(Exception):
        pipeline.extract("2026-01-01")

    # Verificamos que se intentó 3 veces (según configuramos en @retry)
    assert mock_read_sql.call_count == 3
