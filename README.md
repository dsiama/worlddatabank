# Estrutura do Workspace Fabric (Gapminder / World Data Bank)

Este documento descreve a organização de pastas e artefactos presentes no workspace Fabric apresentado.

## 📁 Raiz do Projeto

```
gapminder
```
Workspace principal contendo artefactos analíticos e de ingestão de dados.

---

## 📊 Artefactos de BI

### `gapMinder.Report`
- Relatório Power BI
- Camada de visualização dos dados

### `gapMinder.SemanticModel`
- Modelo semântico
- Contém medidas, relações e estrutura lógica de dados

---

## 📓 Notebooks de Processamento (Medallion Architecture)

### 🔸 Bronze Layer
```
gapminder_BRONZE.Notebook
```
- Ingestão de dados brutos
- Dados na forma original, sem transformações significativas

### 🔹 Silver Layer
```
gapminder_SILVER.Notebook
```
- Limpeza e transformação de dados
- Normalização e preparação para consumo

### 🟡 Gold Layer
```
gapminder_GOLD.Notebook
```
- Dados agregados e prontos para análise
- Otimizados para reporting e consumo final

### ▶️ Orquestração
```
gapminder_runner.Notebook
```
- Executa os notebooks em sequência
- Implementa pipeline end-to-end

---

## 🗄️ Lakehouses

### `worldDataBank_lh.Lakehouse`
- Lakehouse principal
- Armazena dados estruturados por camadas (bronze, silver, gold)

---

## 📁 Organização por Domínio: `world_data_bank`

### 🔸 Bronze
```
bronze/
```
Contém dados brutos ingeridos:
- `WorldDataBank_lh_bronze...`
- `WorldDataBank_nb_bronze_*`

### 🔹 Silver
```
silver/
```
Dados transformados:
- `WorldDataBank_lh_silver...`
- Notebooks de dimensões (`dim`), factos (`facts`) e conjuntos (`set`)

Subpastas incluem:
- `nb_silver_dim` → dimensões
- `nb_silver_fact` → factos
- `nb_silver_set` → conjuntos intermédios

### 🟡 Gold
```
gold/
```
- Dados finais para análise
- Normalmente usados por relatórios

---

## ⚙️ Ficheiros Técnicos

### `.platform`
- Metadados do ambiente Fabric

### `notebook-content.py`
- Código Python interno do notebook

---

## 📄 Outros

### `LICENSE.md`
- Licença do projeto

---

## 🧠 Resumo da Arquitetura

O projeto segue a arquitetura **Medallion**:

| Camada  | Função |
|--------|--------|
| Bronze | Ingestão de dados brutos |
| Silver | Limpeza e transformação |
| Gold   | Dados prontos para consumo |

Fluxo:
```
Fonte de dados → Bronze → Silver → Gold → Semantic Model → Report
```

---

## ✅ Boas práticas observadas

- Separação clara por camadas (Medallion)
- Uso de notebooks para ETL
- Lakehouse como armazenamento central
- Semantic Model desacoplado do Report

---

## 🚀 Possíveis melhorias

- Adicionar pipelines (Data Factory) para orquestração formal
- Implementar versionamento de dados
- Monitorização de execução
- Naming conventions consistentes

