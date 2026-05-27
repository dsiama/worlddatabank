# Princípios Arquitecturais

Princípios estratégicos que guiam todas as decisões de implementação nos pipelines Fabric. Qualquer novo pipeline deve ser avaliado contra estes critérios.

---

## Thin Pipelines, Fat Notebooks

Ferramentas proprietárias de plataforma (Fabric Pipelines, Data Factory Gen2, Dataflow Gen2) representam vendor lock-in e devem ser usadas **apenas como scheduler e trigger básico** — "apenas chama o Notebook."

Toda a lógica de transformação, orquestração de sub-tarefas e validação de dados vive dentro de notebooks Python/PySpark:

- O runner é um notebook que invoca outros notebooks via `notebookutils.notebook.run()` — não um pipeline visual
- As transformações são código Python explícito, não actividades visuais de "Lookup", "Filter", "ForEach"
- O código é testável localmente (`running_local = True`) sem dependência da plataforma

**Status:** implementado — ver [Orquestração Runner](orquestracao-runner.md) e [Visão Geral — Sem Vendor Lock-in](visao-geral.md#sem-vendor-lock-in--notebooks-em-vez-de-data-factory--dataflow-gen2).

---

## Minimizar Dependência de Componentes Proprietários (Low-Code)

Evitar assets proprietários específicos de plataforma. Quando utilizados, providenciar forma de rapidamente serem portados para lógica de código aberto.

Privilegiar:
- Notebooks Python/PySpark
- SQL standard
- Bibliotecas Python open source (`requests`, `json`, `re`, etc.)

**Status:** implementado — sem Data Factory Gen2, sem Dataflow Gen2, sem conectores visuais.

---

## Abstracção da Camada de Dados

### Formato aberto e portável

Utilizar **Delta Lake** como formato de armazenamento — standard de mercado aberto, compatível com Databricks, Azure Synapse, e outros engines além do Fabric.

**Status:** implementado — todas as tabelas Silver e Gold estão em formato Delta.

### Caminhos relativos e parametrizados

Utilizar caminhos abstractos e configuráveis para facilitar a troca de storage no futuro. Nenhum path hardcoded nos notebooks — todos chegam via `main_set`:

```python
# ✅ Parametrizado — troca de Lakehouse sem tocar no código
lh_bronze = main_set["lh_bronze"]

# ❌ Hardcoded — impossível portar
lh_bronze = "abfss://33f78fb2-abc@onelake.dfs.fabric.microsoft.com/<guid>.Lakehouse"
```

**Status:** implementado — ver [Decisões Arquitecturais](visao-geral.md#tudo-parametrizável--sem-valores-hardcoded).

---

## Orquestração Baseada em Metadados (Metadata-Driven)

Privilegiar pipelines genéricos e reutilizáveis: uma pipeline master que entende as suas transformações a partir de metadados (JSON, tabelas SQL), em vez de uma pipeline por transformação.

**Status parcial:**
- Os runners actuais têm a configuração declarativa em Python (`main_set` com grupos e tasks) — estrutura pronta para externalização
- O Gapminder já lê configuração de ficheiros JSON no Lakehouse via `read_config_pipelines()`
- **Trabalho futuro:** mover a configuração de todos os runners para tabelas de metadados Delta, eliminando a necessidade de editar código para alterar comportamento de pipelines

---

## Controlo de Versão — DevOps

As lógicas dos workloads não devem estar presas dentro da plataforma PaaS/SaaS — devem estar num repositório de código com integração Git.

- Utilizar as integrações Git do Fabric para manter um ambiente de DevOps
- Notebooks versionados em Git, não apenas no workspace Fabric
- Esta wiki também deve estar num repositório Git (Azure DevOps)

**Status:** a implementar — os notebooks e esta documentação devem ser migrados para Git com pipeline CI/CD.

---

## Observabilidade Agnóstica

Implementar um wrapper de logging transversal a todos os notebooks, que guarda eventos num repositório centralizado independente da plataforma.

Dados guardados por evento:

| Campo | Descrição |
|---|---|
| `correlation_id` | ID único da execução do pipeline |
| `pipeline_id` | Nome do pipeline |
| `notebook` | Notebook que gerou o evento |
| `step_name` | Passo dentro do notebook |
| `level` | `info`, `warn`, `error`, `dump` |
| `message` | Mensagem do evento |
| `s_from_start` | Segundos desde o início da execução |
| `timestamp` | UTC |

Os logs ficam em Delta Lake (`common_lh_observability`), consultáveis via SQL em qualquer ferramenta — não dependem do Fabric para ser lidos.

**Status:** implementado — ver [ELogger](bibliotecas-comuns.md#dap_eloggerpy).

---

## Modelo Semântico Agnóstico

Aproximar a camada Gold ao modelo semântico, de forma a que o modelo Power BI seja o mais fino possível (cálculos DAX mínimos, métricas já produzidas em Gold).

Balancear:
- Métricas já calculadas em Gold (portáveis, reutilizáveis por qualquer ferramenta BI)
- Funções DAX específicas do Power BI (potência analítica, já adoptadas na ARTE)

**Status:** implementado nas tabelas Gold actuais. A definição do equilíbrio correcto é caso-a-caso.

---

## Governação Portável

Implementar uma estratégia de governação que não dependa de uma ferramenta específica. Utilizar os **metadados do Delta Lake** como fonte de verdade para governação, de forma a que ferramentas externas (Microsoft Purview, Unity Catalog, Apache Atlas/Amundsen, etc.) possam consumir esses metadados sem integração proprietária.

**Status:** a implementar — os dados estão em Delta (metadados disponíveis), mas não existe ainda uma estratégia formal de governação configurada.

---

[← Visão Geral](visao-geral.md) · [Home](../Home.md)
