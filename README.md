# Bitcoin Prediction Market

Projeto de portfólio para aprender engenharia de dados: coleta os mercados
"Bitcoin Up or Down" de 15 minutos da Polymarket e o preço do BTC na Binance,
para estudar como o preço do favorito, o tempo restante e o movimento do BTC
se relacionam com o resultado.

**Estado:** fase 1 em andamento. A coleta funciona; faltam o acompanhamento e a rodada longa.

## Como rodar

Requer [uv](https://docs.astral.sh/uv/). Execute a partir da raiz do projeto.

```bash
uv sync                      # cria o ambiente e instala as dependências
uv run python -m btc_market  # coleta; Ctrl+C para parar
uv run pytest                # testes
uv run ruff check            # estilo e erros comuns
uv run ruff format --check   # formatação
```

A coleta grava em `data/btc_market.db` e registra em `logs/btc_market.log`.
Interrupções (queda de rede, programa parado) ficam na tabela `collector_gap`:
exclua esses intervalos de qualquer análise. Rode uma instância por vez.

