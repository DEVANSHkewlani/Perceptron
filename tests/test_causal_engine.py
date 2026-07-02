import pytest
from unittest.mock import AsyncMock, MagicMock
from world_model.causal_engine import CausalEngine


@pytest.fixture
def engine():
    e = CausalEngine.__new__(CausalEngine)
    e._driver = MagicMock()
    return e


def make_mock_session(data: list):
    mock_result = AsyncMock()
    mock_result.data = AsyncMock(return_value=data)
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=mock_result)
    driver = MagicMock()
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    driver.session.return_value.__aexit__  = AsyncMock(return_value=False)
    return driver, mock_session


@pytest.mark.asyncio
async def test_blast_radius_returns_dependents(engine):
    data = [
        {"affected_id": "svc:auth", "entity_type": "svc", "hop_distance": 1, "rel_type": "DEPENDS_ON"},
    ]
    engine._driver, _ = make_mock_session(data)
    results = await engine.get_blast_radius("db:postgres-primary")
    assert len(results) > 0
    assert results[0].affected_entity_id == "svc:auth"
    assert results[0].hop_distance == 1


@pytest.mark.asyncio
async def test_causal_chain_returns_dependency_path(engine):
    data = [
        {"from_entity": "svc:auth", "to_entity": "db:postgres",
         "rel_type": "DEPENDS_ON", "confidence": None},
    ]
    engine._driver, _ = make_mock_session(data)
    results = await engine.get_causal_chain("svc:auth")
    assert results[0].from_entity == "svc:auth"
    assert results[0].to_entity == "db:postgres"


@pytest.mark.asyncio
async def test_blast_radius_empty_when_no_dependents(engine):
    engine._driver, _ = make_mock_session([])
    results = await engine.get_blast_radius("svc:leaf-service")
    assert results == []
