# Gapminder — Camada Gold

**Notebook:** `gapminder_GOLD`

Padrão genérico: [Metodologia — Camada Gold](../metodologia/camada-gold.md)

---

## Responsabilidade

Consolida as tabelas silver, enriquece a dimensão geográfica com hierarquias de região, cria a dimensão de datas (1800–2100), calcula o `year_rank_index` para ranking temporal, e actualiza o modelo semântico Power BI no final.

---

## Fluxo de Execução

```
1. Ler todas as tabelas silver via ABFSS
        ↓
2. Consolidar dimensão geográfica
   → Left join: dim_geo + world_4region + un_sdg_region
   → gapminder_gold_dim_geo
        ↓
3. Migrar dimensões de metadata (passthrough)
   → gapminder_gold_dim_indicators
   → gapminder_gold_rel_indicator_tags
        ↓
4. Processar factos
   → Deduplicar: dropDuplicates(["geo_id", "year", "indicator_id"])
   → Inner join com gold_dim_geo (garante integridade referencial)
   → Calcular year_rank_index (ver secção abaixo)
   → Escrever particionado por continent e year, com V-Order
   → gapminder_gold_analytics_fact
        ↓
5. Criar dimensão de datas
   → 301 linhas (1800 a 2100 inclusive)
   → Colunas: date_id (int), year (int), period (string: "Past", "Present", "Future")
   → gapminder_gold_dim_date
        ↓
6. Trigger Semantic Model refresh (Power BI)
   → Aguarda conclusão do refresh antes de terminar
```

---

## `year_rank_index` — Ranking Temporal

Window function que calcula, para cada `(indicator_id, geo_id)`, a posição relativa de cada registo ano a ano:

- **Anos futuros** (year > ano_actual): recebem `year_rank_index = 0`
- **Anos passados/presente**: recebem `1, 2, 3, ...` do mais recente para o mais antigo

Permite que relatórios Power BI mostrem facilmente "o valor mais recente" (rank = 1) sem filtros complexos.

---

## Dimensão de Datas (`gapminder_gold_dim_date`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `date_id` | `int` | Identificador numérico (igual ao ano) |
| `year` | `int` | Ano (1800–2100) |
| `period` | `string` | `"Past"` (< ano_actual), `"Present"` (== ano_actual), `"Future"` (> ano_actual) |

---

## Tabelas Escritas no Gold

| Tabela | Conteúdo |
|--------|----------|
| `gapminder_gold_analytics_fact` | Factos analíticos com year_rank_index, particionado por continent e year |
| `gapminder_gold_dim_geo` | Países com hierarquias de região consolidadas |
| `gapminder_gold_dim_indicators` | Indicadores |
| `gapminder_gold_rel_indicator_tags` | Relação indicador → tag |
| `gapminder_gold_dim_date` | 301 anos (1800–2100) com período |

Todas as tabelas são escritas com V-Order activado.

---

## Semantic Model Refresh

No final da execução Gold, é feito trigger do refresh do modelo semântico Power BI (via `semantic_model_refresh_wait` da biblioteca `dap_fabric_semantic_tools.py`). O notebook aguarda a conclusão do refresh antes de devolver o resultado ao runner.

---

## Resultado Devolvido ao Runner

```json
{ "new_data": 1 }
```

---

[← Silver](camada-silver.md) · [Overview](overview.md) · [Home](../Home.md)
