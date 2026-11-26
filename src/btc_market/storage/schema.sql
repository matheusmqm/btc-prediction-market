-- Horários (_ms): UTC em milissegundos.

-- Um registro por mercado de 15 minutos
CREATE TABLE IF NOT EXISTS market (
    id             INTEGER PRIMARY KEY,     -- chave curta usada pelos snapshots
    slug           TEXT    NOT NULL UNIQUE, -- nome na Gamma; consulta o resultado, evita duplicar
    condition_id   TEXT    NOT NULL UNIQUE, -- identifica o mercado nas mensagens do WebSocket
    up_token_id    TEXT    NOT NULL,        -- assinar o lado Up e reconhecer suas mensagens
    down_token_id  TEXT    NOT NULL,        -- o mesmo para o lado Down
    start_ms       INTEGER NOT NULL,        -- define o preço de abertura do BTC
    end_ms         INTEGER NOT NULL,        -- calcula o tempo restante
    discovered_ms  INTEGER NOT NULL,        -- mostra se o mercado foi achado antes de começar
    -- preenchidos depois do fechamento
    outcome        TEXT    CHECK (outcome IN ('up', 'down')), -- quem venceu
    price_to_beat  REAL,                    -- preço inicial oficial (Chainlink)
    final_price    REAL,                    -- preço final oficial; mostra resultados apertados
    resolved_ms    INTEGER,                 -- quando soubemos o resultado
    CHECK (end_ms > start_ms)
) STRICT;

-- Uma linha a cada mudança de bid ou ask de um lado
CREATE TABLE IF NOT EXISTS market_snapshot (
    id           INTEGER PRIMARY KEY,       -- mantém a ordem de chegada no mesmo milissegundo
    market_id    INTEGER NOT NULL REFERENCES market (id), -- liga ao mercado e ao resultado
    side         TEXT    NOT NULL CHECK (side IN ('up', 'down')), -- de qual lado é o preço
    bid          REAL,                      -- melhor compra; mostra o spread
    ask          REAL,                      -- quanto custa entrar
    ask_size     REAL,                      -- volume à venda no momento da mudança
    source_ms    INTEGER NOT NULL,          -- quando ocorreu; cruza com o BTC
    received_ms  INTEGER NOT NULL           -- quando chegou ao PC; mede o atraso
) STRICT;

-- Busca rápida por mercado e instante
CREATE INDEX IF NOT EXISTS market_snapshot_by_time
    ON market_snapshot (market_id, source_ms);

-- Um registro por negócio agregado da Binance
CREATE TABLE IF NOT EXISTS btc_price (
    agg_trade_id  INTEGER PRIMARY KEY,      -- evita duplicados e revela negócios faltando
    price         REAL    NOT NULL,         -- calcula a distância desde a abertura
    trade_ms      INTEGER NOT NULL,         -- quando o negócio ocorreu
    received_ms   INTEGER NOT NULL          -- quando chegou ao PC; mede o atraso
) STRICT;

-- Busca o preço de um instante
CREATE INDEX IF NOT EXISTS btc_price_by_time ON btc_price (trade_ms);

-- Períodos em que a coleta falhou
CREATE TABLE IF NOT EXISTS collector_gap (
    id        INTEGER PRIMARY KEY,          -- identifica a lacuna para poder fechá-la
    source    TEXT    NOT NULL CHECK (source IN ('binance', 'polymarket')), -- qual coleta falhou
    start_ms  INTEGER NOT NULL,             -- início do trecho a excluir da análise
    end_ms    INTEGER,                      -- fim; vazio enquanto não volta
    reason    TEXT    NOT NULL,             -- motivo, para corrigir a causa
    CHECK (end_ms IS NULL OR end_ms >= start_ms)
) STRICT;
