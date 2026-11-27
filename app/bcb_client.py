"""
Módulo compartilhado: conexão com o Postgres da Aiven e leitura de dados
de fontes abertas (IBGE/SIDRA). Usado por app.py, producer.py e
consumer.py — evita duplicar a lógica de acesso a dados em cada script.

O nome do módulo (bcb_client) foi mantido para não precisar mexer nos
outros arquivos.
"""

import os
import datetime
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
import requests

DB_URL = os.environ.get("AIVEN_DB_URL")

# Os códigos são ids internos nossos (usados na tabela bcb_series).
SERIES = {
    1006: {
        "name": "Taxa de desocupação (PNAD, trimestre móvel)",
        "unit": "%",
        "casas": 1,
        "source": "ibge",
        "tabela": 6381,
        "variavel": 4099,
    },
}
POINTS_PER_SERIES = 30

_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        if not DB_URL:
            raise RuntimeError("AIVEN_DB_URL não configurada")
        _pool = psycopg2_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DB_URL,
            sslmode="require",
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )
    return _pool


@contextmanager
def get_connection():
    """Pega uma conexão emprestada do pool e devolve ao sair do bloco."""
    conn = _get_pool().getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)


def init_db():
    """Cria as tabelas caso ainda não existam (tolerando a corrida entre
    workers do gunicorn na inicialização)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bcb_series (
                        series_code INTEGER NOT NULL,
                        ref_date    DATE NOT NULL,
                        value       NUMERIC NOT NULL,
                        fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (series_code, ref_date)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS visits (
                        id SERIAL PRIMARY KEY,
                        path TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
            conn.commit()
    except psycopg2.errors.DuplicateTable:
        pass
    except psycopg2.errors.UniqueViolation as exc:
        if "pg_type" not in str(exc):
            raise


# ---------------------------------------------------------------------------
# Fontes de dados
# ---------------------------------------------------------------------------

def _fetch_ibge(meta: dict, n: int):
    url = (
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{meta['tabela']}"
        f"/periodos/-{n}/variaveis/{meta['variavel']}"
    )
    resp = requests.get(url, params={"localidades": "N1[all]"}, timeout=15)
    resp.raise_for_status()
    serie = resp.json()[0]["resultados"][0]["series"][0]["serie"]  # {"202609": "5.6", ...}
    parsed = []
    for period, val in sorted(serie.items()):
        try:
            value = float(val)
        except ValueError:  # IBGE usa "-" ou "..." quando não há dado
            continue
        year = int(period[:4])
        part = int(period[4:6])
        # trimestral: 01..04 -> primeiro mês do trimestre; mensal: usa o mês direto
        month = (part - 1) * 3 + 1 if meta.get("freq") == "trimestral" else part
        parsed.append({"ref_date": datetime.date(year, month, 1), "value": value})
    return parsed[-n:]


_FETCHERS = {
    "ibge": _fetch_ibge,
}


def fetch_series(series_code: int, n: int = POINTS_PER_SERIES):
    """Busca os últimos N pontos de uma série na fonte configurada em SERIES."""
    meta = SERIES[series_code]
    try:
        return _FETCHERS[meta["source"]](meta, n)
    except (KeyError, IndexError, TypeError) as exc:
        # normaliza respostas inesperadas para ValueError, que o app.py já trata
        raise ValueError(f"Resposta inesperada da fonte '{meta['source']}': {exc}")


# Alias para não precisar mexer em app.py e producer.py
fetch_series_from_bcb = fetch_series


def upsert_series(series_code: int, points: list):
    """Grava (ou atualiza) pontos de uma série direto no Postgres."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            for p in points:
                cur.execute(
                    """
                    INSERT INTO bcb_series (series_code, ref_date, value, fetched_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (series_code, ref_date)
                    DO UPDATE SET value = EXCLUDED.value, fetched_at = now();
                    """,
                    (series_code, p["ref_date"], p["value"]),
                )
        conn.commit()
