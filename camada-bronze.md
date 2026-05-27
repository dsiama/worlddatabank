# Gapminder — Camada Bronze

**Notebook:** `gapminder_BRONZE`

Padrão genérico: [Metodologia — Camada Bronze](../metodologia/camada-bronze.md)

---

## Responsabilidade

Ingere o catálogo de indicadores e os dados históricos da Gapminder REST API e escreve tabelas Delta no Lakehouse Bronze. Não há ficheiros intermédios — os dados vêm directamente da API.

---

## Fluxo de Execução

```
1. Ler catálogo de conceitos
   → GET /concepts
   → filtrar concept_type == "measure"
   → dicionário {indicator_id: indicator_name} (~482 indicadores)
        ↓
2. Ler dimensões geográficas
   → GET /entities/geo              → bronze_dim_geo
   → GET /entities/geo/world_4region → bronze_dim_geo_world_4region
   → GET /entities/geo/un_sdg_region → bronze_dim_geo_un_sdg_region
        ↓
3. Processar indicadores em batches de 100
   Para cada batch:
     Para cada indicator_id:
       → GET /datapoints/{indicator_id}/geo/time
       → parsear CSV: colunas geo, time, value
       → adicionar coluna indicator_id e _ingested_at
       → acumular no batch
   → escrever batch em gapminder_bronze_fact (Delta, DELETE+APPEND por batch)
        ↓
4. Escrever dimensão de conceitos
   → bronze_dim_concepts (~528 linhas)
        ↓
5. Retornar { dim_records: 528, fact_records: 3704392, new_data: 1 }
```

---

## Tabelas Escritas no Bronze

| Tabela | Conteúdo | Registos |
|--------|----------|----------|
| `gapminder_bronze_fact` | Dados históricos: `geo`, `time`, `value`, `indicator_id`, `_ingested_at` | ~3.7M |
| `bronze_dim_concepts` | Catálogo de todos os conceitos | ~528 |
| `bronze_dim_geo` | Países e territórios | ~273 |
| `bronze_dim_geo_world_4region` | 4 macro-regiões mundiais | 4 |
| `bronze_dim_geo_un_sdg_region` | Regiões ONU SDG | — |

---

## Padrão de Escrita

Os facts são escritos em **batches de 100 indicadores**:
- Se a tabela já existe: DELETE dos registos com `indicator_id IN (batch_ids)` + APPEND
- Se a tabela não existe: OVERWRITE (criação inicial)

Este padrão permite reprocessamento parcial por indicator sem reescrever toda a tabela.

---

## Logging

O ELogger regista o resultado de cada batch com:
- Número de indicadores processados
- Número de registos escritos
- Tempo de execução do batch

O tempo total de Bronze é ~949s (~16 min) para os ~482 indicadores.

---

## Resultado Devolvido ao Runner

```json
{ "dim_records": 528, "fact_records": 3704392, "new_data": 1 }
```

Se `new_data == 0`, o runner pode condicionar os steps seguintes (Silver, Gold) via `run_withouth_new_data`.

---

[← Fontes](fontes-dados.md) · [Silver →](camada-silver.md) · [Home](../Home.md)
