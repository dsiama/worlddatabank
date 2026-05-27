# Gapminder — Camada Silver

**Notebook:** `gapminder_SILVER`

Padrão genérico: [Metodologia — Camada Silver](../metodologia/camada-silver.md)

---

## Responsabilidade

Normaliza os dados bronze, resolve sufixos K/M/B nos valores, filtra o intervalo temporal, cria dimensões estruturadas e escreve tabelas Delta Silver com V-Order activado para optimização Power BI.

---

## Fluxo de Execução

```
1. Processar dimensão de indicadores
   → gapminder_silver_dim_indicators   (482 registos)
   → gapminder_silver_rel_indicator_tags  (relação indicador → tags, ~71 tags distintas)
        ↓
2. Processar dimensão geográfica
   → Join de bronze_dim_geo com macro-regiões (world_4region, un_sdg_region)
   → Cast coordenadas (lat, lon) para double
   → gapminder_silver_dim_geo   (273 países)
        ↓
3. Processar factos
   → Resolver sufixos K/M/B nos valores (ver secção abaixo)
   → Filtrar: remover registos com year fora de [1800, 2100]
   → Remover valores NaN (registos de anos futuros sem dados)
   → Escrever particionado por indicator_id com V-Order
   → gapminder_silver_fact
```

---

## Resolução de Sufixos K/M/B

Os valores da API Gapminder chegam como strings com sufixos de magnitude:

| Sufixo | Multiplicador | Exemplo |
|--------|--------------|---------|
| `k` (case-insensitive) | × 1.000 | `"3.5k"` → `3500.0` |
| `M` | × 1.000.000 | `"2.1M"` → `2100000.0` |
| `B` | × 1.000.000.000 | `"1.5B"` → `1500000000.0` |
| Sem sufixo | × 1 | `"42.3"` → `42.3` |

Implementado com UDF PySpark que detecta o sufixo no último caractere da string e aplica o multiplicador correspondente. Valores não reconhecidos resultam em `null`.

---

## Filtro Temporal

- **Intervalo aceite:** 1800 ≤ year ≤ 2100
- **Registos futuros (year > ano_actual) com NaN** são removidos — representam projecções ainda não disponíveis

---

## Tags de Indicadores

A coluna `tags` dos conceitos é explodida em registos individuais:
- `gapminder_silver_rel_indicator_tags` — uma linha por par `(indicator_id, tag)`
- ~71 tags distintas no total

---

## V-Order (Optimização Power BI)

Todas as tabelas silver são escritas com V-Order activado:

```python
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
```

V-Order reorganiza os dados internamente para leitura mais rápida no DirectLake (Power BI). Não altera o schema nem o conteúdo.

---

## Tabelas Escritas no Silver

| Tabela | Conteúdo | Registos |
|--------|----------|----------|
| `gapminder_silver_fact` | Factos normalizados, particionado por `indicator_id` | ~3.7M |
| `gapminder_silver_dim_indicators` | Indicadores (type="measure") | 482 |
| `gapminder_silver_rel_indicator_tags` | Relação indicador → tag | 482× |
| `gapminder_silver_dim_geo` | Países com macro e micro regiões | 273 |

---

## Resultado Devolvido ao Runner

```json
{ "new_data": 1 }
```

---

[← Bronze](camada-bronze.md) · [Gold →](camada-gold.md) · [Home](../Home.md)
