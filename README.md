# Wiki — Pipelines Fabric

Documentação da metodologia de pipelines de dados em Microsoft Fabric e dos case studies implementados.

> **Princípios:** [Thin Pipelines, Fat Notebooks — Princípios Arquitecturais](metodologia/principios.md)

---

## Metodologia

Padrões genéricos aplicáveis a qualquer pipeline Fabric, derivados das implementações concretas.

| Secção | Conteúdo |
|--------|----------|
| [Princípios Arquitecturais](metodologia/principios.md) | Thin Pipelines / Fat Notebooks, Delta Lake, Metadata-Driven, Observabilidade |
| [Visão Geral](metodologia/visao-geral.md) | Arquitectura Medallion, quando usar cada camada |
| [Camada Bronze](metodologia/camada-bronze.md) | Ingestão de dados externos, consume-and-delete, upload por chunks |
| [Camada Silver](metodologia/camada-silver.md) | Normalização, cast functions, colunas de auditoria, error handling |
| [Camada Gold](metodologia/camada-gold.md) | Consolidação, delete+append, mapeamentos, enriquecimento |

---

### [Gapminder](gapminder/overview.md)

Indicadores económicos e sociais mundiais. Ingestão via REST API pública, ~3.7M registos, semantic model refresh Power BI no final.

| Secção | |
|--------|--|
| [Overview](gapminder/overview.md) | Arquitectura, notebooks, tempos de execução |
| [Fontes de Dados](gapminder/fontes-dados.md) | Gapminder REST API, endpoints, catálogo de indicadores |
| [Bronze](gapminder/camada-bronze.md) | Pull via API, batches de 100 indicadores, tabelas Bronze |
| [Silver](gapminder/camada-silver.md) | Sufixos K/M/B, filtro 1800–2100, V-Order, dim_geo |
| [Gold](gapminder/camada-gold.md) | year_rank_index, dim_date, semantic model refresh |

---

