from pathlib import Path

from btc_market.config import Config


def test_database_lives_inside_data_dir():
    config = Config(data_dir=Path("somewhere"))

    assert config.database_path == Path("somewhere") / "btc_market.db"


def test_sources_use_secure_connections():
    config = Config()

    assert config.gamma_url.startswith("https://")
    assert config.polymarket_ws_url.startswith("wss://")
    assert config.binance_ws_url.startswith("wss://")
