# Gapminder — Fontes de Dados

Padrão de ingestão: [Metodologia — Fontes de Dados](../metodologia/fontes-de-dados.md)

---

## Gapminder REST API

API pública, sem autenticação. O pipeline usa HTTP GET simples via `requests`.

### Endpoints Utilizados

| Endpoint | O que devolve |
|----------|---------------|
| `/concepts` | Catálogo de todos os conceitos disponíveis (~528 registos) |
| `/entities/geo` | Dimensão geográfica — países e territórios (~273 entidades) |
| `/entities/geo/world_4region` | Macro-regiões mundiais (4 regiões) |
| `/entities/geo/un_sdg_region` | Regiões ONU SDG |
| `/datapoints/<indicator_id>/geo/time` | Dados históricos de um indicador por país e ano |

### Volume de Dados

| Tabela | Registos |
|--------|----------|
| Conceitos (dim_concepts) | ~528 |
| Indicadores do tipo "measure" | ~482 |
| Factos (bronze_fact) | ~3.7 milhões |
| Países (dim_geo) | ~273 |

---

## Catálogo de Indicadores

A função `get_gapminder_indicators_dict()` lê o endpoint `/concepts` e filtra apenas os registos com `concept_type == "measure"`. O resultado é um dicionário `{indicator_id: indicator_name}` com ~482 entradas.

Estes são os indicadores para os quais o pipeline vai buscar dados históricos.

---

## Padrão de Acesso (Pull via API)

```python
def extract_indicator(indicator_id):
    url = f"{BASE_URL}/datapoints/{indicator_id}/geo/time"
    response = requests.get(url)
    # parsear CSV: colunas geo, time, value
    return dataframe_com_geo_time_value
```

Os dados chegam em CSV com 3 colunas: `geo` (código de país), `time` (ano), `value` (valor do indicador).

---

## Sem Ficheiros Intermediários

Ao contrário do pipeline Calamidade, o Gapminder não usa Azure Blob Storage como staging. Os dados são obtidos directamente da API REST e escritos em Delta no Lakehouse.

---

[← Overview](overview.md) · [Bronze →](camada-bronze.md) · [Home](../Home.md)
