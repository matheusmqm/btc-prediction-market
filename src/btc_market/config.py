"""Configuração da coleta. Não há credenciais: os valores padrão bastam."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    gamma_url: str = "https://gamma-api.polymarket.com"
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

    # Caminhos relativos à pasta de onde o programa é executado (a raiz do projeto).
    data_dir: Path = Path("data")
    log_dir: Path = Path("logs")

    flush_interval_seconds: float = 1.0
    ping_interval_seconds: float = 10.0
    # Sem nenhuma mensagem por este tempo, a conexão é refeita.
    binance_silence_seconds: float = 15.0  # há negócios a cada poucos segundos
    polymarket_silence_seconds: float = 25.0  # há PONG a cada ping
    subscribe_lead_seconds: int = 60  # antecedência para assinar o próximo mercado
    unsubscribe_delay_seconds: int = 5  # espera após o fim antes de cancelar o mercado
    resolution_interval_seconds: float = 30.0  # entre consultas por resultados pendentes
    final_price_wait_hours: int = 24  # depois disso, deixa de esperar o preço final

    @property
    def database_path(self) -> Path:
        return self.data_dir / "btc_market.db"
