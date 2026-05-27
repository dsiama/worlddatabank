# Gapminder

Pipeline de ingestão de indicadores económicos e sociais mundiais a partir da API pública do [Gapminder](https://www.gapminder.org/). Ingere ~482 indicadores e ~3.7 milhões de registos de factos históricos (1800–2100), organiza em tabelas analíticas Gold, e actualiza o modelo semântico Power BI no final.

Metodologia utilizada: [Visão Geral da Metodologia](../metodologia/visao-geral.md)

---

## Fontes

| Fonte | Tipo | Descrição |
|-------|------|-----------|
| Gapminder REST API | REST API pública | Catálogo de indicadores + dados históricos por indicador |

Ver detalhe: [Fontes de Dados](fontes-dados.md)

---

## Arquitectura

```
Gapminder REST API (pública, sem autenticação)
  ├── /concepts → catálogo de indicadores (~528 conceitos, ~482 do tipo "measure")
  ├── /entities/geo → dimensões geográficas (países, regiões)
  └── /datapoints/<indicator_id> → dados históricos por indicador
       │  HTTP GET → CSV parsing → Delta write
       ▼
BRONZE — Lakehouse
  Tables/gapminder_bronze_fact         (~3.7M registos)
  Tables/bronze_dim_concepts           (~528 linhas)
  Tables/bronze_dim_geo                (países)
  Tables/bronze_dim_geo_world_4region  (4 macro-regiões)
  Tables/bronze_dim_geo_un_sdg_region  (regiões SDG ONU)
       │  Normalização · K/M/B suffix · V-Order · filtro temporal
       ▼
SILVER — Lakehouse
  Tables/gapminder_silver_fact         (particionado por indicator_id)
  Tables/gapminder_silver_dim_indicators
  Tables/gapminder_silver_rel_indicator_tags
  Tables/gapminder_silver_dim_geo
       │  Join de dimensões · dedup · year_rank_index
       ▼
GOLD — Lakehouse
  Tables/gapminder_gold_analytics_fact
  Tables/gapminder_gold_dim_geo
  Tables/gapminder_gold_dim_indicators
  Tables/gapminder_gold_rel_indicator_tags
  Tables/gapminder_gold_dim_date       (301 anos: 1800–2100)
       │  Semantic model refresh (Power BI)
```

---

## Notebooks

| Notebook | Papel |
|----------|-------|
| `gapminder_runner` | Orquestrador — único ponto de entrada |
| `gapminder_BRONZE` | Bronze — ingere catálogo + dados via API |
| `gapminder_SILVER` | Silver — normaliza, desambigua sufixos K/M/B, filtra |
| `gapminder_GOLD` | Gold — enriquece dimensões, cria dim_date, SM refresh |

---

## Tempos de Execução Observados

| Notebook | Tempo |
|----------|-------|
| Bronze | ~949s (~16 min) |
| Silver | ~115s |
| Gold | ~53s |

---

## Documentação Detalhada

- [Fontes de Dados](fontes-dados.md)
- [Camada Bronze](camada-bronze.md)
- [Camada Silver](camada-silver.md)
- [Camada Gold](camada-gold.md)

---

[Home](../Home.md)
