# Camada Gold — Consolidação

A camada Gold harmoniza dados de múltiplas fontes silver, aplica mapeamentos, e produz a tabela analítica final. É o destino para relatórios e modelos semânticos (Power BI).

Para implementações concretas, ver:
- [Calamidade — Gold](../calamidade/camada-gold.md) — `gold_fact_candidaturas`
- [Gapminder — Gold](../gapminder/camada-gold.md) — `gapminder_gold_analytics_fact`

---

## Padrão de Escrita por Fonte: Delete + Append

O padrão principal para carregar dados de uma fonte silver para a gold:

```python
# 1. Apagar registos desta fonte
delta = DeltaTable.forPath(spark, gold_path)
delta.delete(F.col("origem").isin("fonte_a", "fonte_b"))

# 2. Fazer append dos novos registos processados
df_gold.write.format("delta").mode("append").save(gold_path)
```

**Porquê delete + append (e não overwrite)?**
- Permite que múltiplas fontes coexistam na mesma tabela gold
- Cada fonte pode ser reprocessada de forma independente
- Não afecta registos de outras fontes durante o reprocessamento

---

## Padrão de Enriquecimento: Overwrite Total

Usado para steps que relêem a tabela gold completa e adicionam colunas calculadas (ex: níveis de uma Decomposition Tree, rankings temporais):

```python
df_gold_full = spark.read.format("delta").load(gold_path)
df_gold_full = df_gold_full.join(df_mapa, chave_join, "left")

df_gold_full.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)
```

**Quando usar overwrite total:** quando é necessário enriquecer todos os registos existentes com novas colunas ou recalcular valores com base num mapeamento actualizado.

---

## Mapeamentos Externos

A gold usa ficheiros CSV de mapeamento para normalizar valores categóricos:

| Padrão | Descrição |
|--------|-----------|
| `mapa_<estados/niveis>.csv` | Ficheiros delimitados por `\|`, lidos como DataFrame e usados em joins |
| Join `left` | Registos sem correspondência ficam com `null` nos campos mapeados (nunca são descartados) |
| Valor por defeito | Definido explicitamente via `F.coalesce(col("mapeado"), F.lit("valor_default"))` |

---

## Dois Contextos de Execução

O mesmo notebook gold pode ser chamado em contextos diferentes, controlados pelo parâmetro `running_steps`:

| Context | `movement_type` no runner | O que faz |
|---------|--------------------------|-----------|
| `silver_to_gold` | `silver_to_gold` | Carrega dados de uma fonte silver para a gold (delete + append) |
| `update_on_gold` | `update_on_gold` | Relê a gold completa e enriquece com colunas calculadas (overwrite total) |

---

## Schema Típico da Tabela Gold

A gold expõe um schema harmonizado e normalizado, independente das diferenças entre as fontes silver:

- Datas em `DateType` ou `TimestampType`
- Valores numéricos em `DoubleType` ou `LongType`
- Strings normalizadas (sem acentos, uppercase quando relevante para joins)
- Coluna `origem` identifica a fonte de cada registo
- Colunas de auditoria herdadas da silver

---

## Resultado Devolvido ao Runner

```json
{
  "status": "OK",
  "results": [
    { "step": "fonte_a", "rows": 35907 },
    { "step": "enrichment_step", "rows": 71442 }
  ]
}
```

---

[← Camada Silver](camada-silver.md) · [Orquestração →](orquestracao-runner.md) · [Home](../Home.md)
