# Visão Geral — Metodologia de Pipelines Fabric

Esta metodologia define como construímos e organizamos pipelines de dados em Microsoft Fabric. É aplicada consistentemente em todos os pipelines do projecto.

---

## Arquitectura Medallion

Todos os pipelines seguem a arquitectura **Bronze → Silver → Gold**:

```
Fonte Externa
     │
     ▼
  BRONZE          Cópia fiel da fonte — sem transformações de schema
     │
     ▼
  SILVER          Normalização: cast de tipos, colunas de auditoria, overwrite
     │
     ▼
  GOLD            Consolidação: harmonização de múltiplas fontes, mapeamentos, enriquecimento
```

### Quando usar cada camada

| Camada | Responsabilidade | Escrita |
|--------|-----------------|---------|
| **Bronze** | Copiar dados externos para o Lakehouse sem alterar o conteúdo | Ficheiros originais (CSV, JSON, etc.) |
| **Silver** | Normalizar schema, cast de tipos, adicionar colunas de auditoria | Delta, mode=overwrite |
| **Gold** | Consolidar múltiplas fontes, aplicar mapeamentos, enriquecer | Delta, delete+append por fonte ou overwrite total |

---

## Organização dos Notebooks

Cada pipeline tem uma pasta de notebooks com esta estrutura típica:

```
notebooks/<pipeline>/
├── <pipeline>_runner.ipynb        — orquestrador, único ponto de entrada
├── <pipeline>_BRONZE.ipynb        — ingestão (ou vários notebooks de bronze por fonte)
├── <pipeline>_SILVER.ipynb        — normalização (ou vários por fonte)
└── <pipeline>_GOLD.ipynb          — consolidação
```

As bibliotecas partilhadas são compiladas a partir de `notebooks/Utils/` e instaladas em `/lakehouse/default/Files/Global_Libs/` no Fabric.

---

## Padrões Transversais

Todos os notebooks do pipeline partilham os mesmos padrões:

- **Parâmetros via `main_set`** — configuração injectada pelo runner em runtime ([ver Orquestração](orquestracao-runner.md))
- **Logging estruturado** — ELogger com `correlation_id` propagado entre notebooks ([ver Bibliotecas](bibliotecas-comuns.md))
- **Error handling** — padrão `handleError` com saída imediata em produção ([ver Silver](camada-silver.md#tratamento-de-erros))
- **Execução local vs. produção** — flag `running_local` controla comportamento de erros e parâmetros

---

## Decisões Arquitecturais

### Sem Vendor Lock-in — Notebooks em vez de Data Factory / Dataflow Gen2

Todos os pipelines são implementados em **Spark notebooks**, não em Data Factory Gen2 pipelines nem Dataflow Gen2. Razões:

- Os notebooks são portáteis — correm localmente, num workspace de desenvolvimento, e em produção sem alterações
- A lógica está em Python/PySpark, não em configuração visual proprietária que não é versionável de forma útil
- O runner invoca notebooks via `notebookutils.notebook.run()` — sem dependências de conectores proprietários
- Sem componentes visuais cujo comportamento muda entre versões do Fabric sem aviso

### Tudo Parametrizável — Sem Valores Hardcoded

**Nunca** se hardcodam paths de Lakehouses, nomes de contentores, credenciais, ou qualquer configuração que varie por ambiente. Tudo passa via `main_set`:

```python
# ✅ Correcto — parametrizado via main_set
lh_bronze = main_set["lh_bronze"]   # abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse

# ❌ Errado — hardcoded
lh_bronze = "abfss://33f78fb2-abc-.../<guid>.Lakehouse"
```

Isto garante que o mesmo notebook corre em desenvolvimento, staging e produção apenas mudando os parâmetros no runner.

### Sempre ABFSS — Sem Montagem de Lakehouses

Os paths de Lakehouse são sempre em formato **ABFSS** (`abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/`). Nunca se "afixa" um Lakehouse em construção de paths — os paths chegam sempre como parâmetro.

A excepção são as **Global Libs**, que estão afixadas como Lakehouse por defeito no contexto dos notebooks. Isto permite o import directo sem configuração:

```python
import sys
sys.path.append("/lakehouse/default/Files/Global_Libs")
from dap_ELogger import ELogger
```

Este `/lakehouse/default/` funciona porque o Lakehouse das Global Libs está configurado como default no workspace. Para tudo o resto, usar ABFSS explícito.

### Separação de Workspaces por Tipo de Acesso

| Workspace | ID | Quando usar |
|-----------|-----|------------|
| `e_ext_ingest_ws` | — | Dados com acesso **público** ou provenientes do **exterior** (APIs públicas, parceiros, upload de ficheiros) |
| `e_int_ingest_ws` | — | Dados com acesso **privado** ou de sistemas internos |

O pipeline Calamidade e o Gapminder correm em `e_ext_ingest_ws`. Pipelines que acedam a sistemas internos ou dados sensíveis devem usar `e_int_ingest_ws`.

---

## Trabalho Futuro

Áreas identificadas para evolução, mas ainda não implementadas:

- **Funções centralizadas:** centralizar funções utilitárias reutilizáveis (normalizar_geo, cast functions, etc.) numa biblioteca partilhada acessível a todos os pipelines. Actualmente estão duplicadas inline em cada notebook silver.
- **Configuração metadata-driven:** mover a configuração de pipelines (grupos, tasks, parâmetros) para tabelas de metadata em vez de estar hardcoded no runner. Permitiria alterar a configuração sem modificar notebooks.

---

---

## Case Studies

Implementações concretas desta metodologia:

- [PAGE — Apoio Calamidade](../calamidade/overview.md) — ingestão de candidaturas via Azure Blob Storage
- [Gapminder](../gapminder/overview.md) — ingestão de indicadores económicos via REST API pública

---

## Documentação da Metodologia

| Secção | Conteúdo |
|--------|----------|
| [Camada Bronze](camada-bronze.md) | Padrões de ingestão de dados externos |
| [Camada Silver](camada-silver.md) | Normalização, casting, auditoria |
| [Camada Gold](camada-gold.md) | Consolidação, mapeamentos, enriquecimento |
| [Orquestração — Runner](orquestracao-runner.md) | Configuração declarativa, grupos, movement types |
| [Bibliotecas Comuns](bibliotecas-comuns.md) | ELogger, Azure REST, Lakehouse file tools |
| [Fontes de Dados](fontes-de-dados.md) | Padrões de acesso a fontes externas |

---

[Voltar à Home](../Home.md)
