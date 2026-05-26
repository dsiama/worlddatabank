# 📦 Documentação Completa - Workspace Fabric (Gapminder / World Data Bank)

---

## 📁 1. Visão Geral

Workspace Fabric estruturado segundo a arquitetura **Medallion (Bronze → Silver → Gold)**, com separação clara entre ingestão, transformação e consumo.

```
Fonte → Bronze → Silver → Gold → Semantic Model → Report
```

---

## 🧩 2. Diagrama da Arquitetura

![Arquitetura Medallion](arquitetura_medallion.png)

---

## 📊 3. Artefactos de BI

### gapMinder.Report
- Relatório Power BI
- Camada de visualização

### gapMinder.SemanticModel
- Modelo semântico
- Define medidas, relações e métricas

---

## 📓 4. Notebooks Globais

### gapminder_runner.Notebook
- Orquestra pipeline completo
- Executa Bronze → Silver → Gold

### gapminder_BRONZE.Notebook
- Controla ingestão global

### gapminder_SILVER.Notebook
- Transformações principais

### gapminder_GOLD.Notebook
- Preparação final para consumo

---

## 🗄️ 5. Lakehouses

### worldDataBank_lh.Lakehouse
- Lakehouse principal

---

## 📁 6. Estrutura por Domínio

```
world_data_bank/
```

---

### 🔸 Bronze Layer

```
bronze/
 ├── WorldDataBank_lh_bronze.Lakehouse
 ├── WorldDataBank_nb_bronze_facts.Notebook
 └── WorldDataBank_nb_localizacoes.Notebook
```

#### 📓 Notebooks

✅ **WorldDataBank_nb_bronze_facts**
- Input: fontes externas (datasets World Bank)
- Output: tabelas raw (factos)

✅ **WorldDataBank_nb_localizacoes**
- Input: dados geográficos
- Output: tabela de localizações

---

### 🔹 Silver Layer

```
silver/
 ├── WorldDataBank_lh_silver.Lakehouse
 │    ├── .platform
 │    ├── alm.settings.json
 │    ├── lakehouse.metadata.json
 │    └── shortcuts.metadata.json
 │
 ├── WorldDataBank_nb_silver_dims.Notebook
 ├── WorldDataBank_nb_silver_facts.Notebook
 └── WorldDataBank_nb_silver_set_import.Notebook
```

#### 📓 Notebooks

✅ **WorldDataBank_nb_silver_dims**
- Input: dados Bronze
- Output: dimensões (país, regiões, indicadores)

✅ **WorldDataBank_nb_silver_facts**
- Input: dados Bronze
- Output: factos limpos e estruturados

✅ **WorldDataBank_nb_silver_set_import**
- Input: datasets intermédios
- Output: datasets preparados para Gold

#### ⚙️ Ficheiros técnicos
- `.platform` → configuração Fabric
- `alm.settings.json` → deployment e lifecycle
- `lakehouse.metadata.json` → definição estrutural
- `shortcuts.metadata.json` → atalhos externos

---

### 🟡 Gold Layer

```
gold/
 └── WorldDataBank_lh_gold.Lakehouse
```

- Dados prontos para consumo
- Base para Semantic Model

---

## 🔄 7. Fluxo de Dados

1. Dados ingeridos para Bronze
2. Limpeza e modelação em Silver
3. Agregação em Gold
4. Consumo via Semantic Model
5. Visualização no Report

---

## ✅ 8. Boas Práticas

- Arquitetura Medallion bem implementada
- Separação clara entre camadas
- Notebooks com responsabilidade isolada
- Lakehouses por camada
- Preparado para escalabilidade

---

## 🚀 9. Próximos Melhoramentos

- Adicionar pipelines Fabric (Data Factory)
- Monitorização de execução
- Data lineage detalhado
- Data catalog automático
- Testes de qualidade de dados

---

## 📌 10. Resumo

Este workspace segue uma abordagem moderna de engenharia de dados:

- Modular
- Escalável
- Orientado a analytics
- Alinhado com boas práticas Microsoft Fabric

