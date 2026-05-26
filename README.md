# 📦 Documentação Completa - Workspace Fabric (Gapminder / World Data Bank)

---

## 📁 1. Visão Geral


# Regras Gerais 

- Minimizar a dependência de componentes proprietários (Low-Code),  concentrar a lógica em código standard.
	
- "Thin Pipelines, Fat Notebooks"
	- Ferramentas proprietárias para a operacionalização da solução do tipo de solução de plataforma (PaaS e DaaS) que representam lockin, devem ser usadas apenas como um scheduler e trigger básico. "Apenas chama o Notebook."
	- Toda a lógica de transformação, orquestração de sub-tarefas e validação de dados deve estar dentro de Notebook/container Python.
	- Ser testado periodicamente a execução deste código em diferentes arquiteturas não plataforma
	- Evitar atividades de "Lookup", "Filter", "ForEach" visuais ou outras muito especificas a plataformas próprias. 
	- Evitar utilização de assets proprietários e quando utilizados, providenciar forma de rapidamente serem portados para uma lógica de código aberto. Privilegiar o uso de Notebook e  código SQL

- Abstração da Camada de Dados
	- Utilizar formato Delta Lake ou similar que seja um standard de mercado aberto e portável
	- Utilizar caminhos relativos para definir pontos de montagem facilitando a troca de storage no futuro.
		
- Orquestração baseada em Metadados (Metadata-Driven)
	- dar prevalência a uma logica de reutilização de pipelines únicos e genéricos:  não desenvolver uma pipeline para cada transformação, utilizar uma lógica de poucas pipelines master que percebem as suas transformações a partir de metadados (JSON, Tabelas de SQL, …)

- Controlo de versão
	- utilizar as integrações com Git para manter um ambiente de DevOps
	- as lógcas dos workloads não devem estar presas dentro da plataforma PaaS / SaaS, mas sim no repositório de código

- Observabilidade Agnóstica
	- implementar uma lógica de notebook wrapper com implementação de uma classe a ser utilizda por todos os outros notebooks e que ao ser utilizada guarda logs num repositório centralizado
	- guardar dados como o run_id, job_name, status, start_time, end_time, rows_affected, assets_affected, error_message, payload
	- guardar esta info num storage de baixa latência e/ou API

- Modelo Semântico Agnóstico
	- Dentro do tecnicamente possível, aproximar a camada de dados Gold ao modelo semântio. Fazer um bom balanceamento relativamente às métricas já produzidas em Gold e a mais valias de utilização do modelo desenvolvido em/com PBI e funções próprias de Dax (ferramentas já usadas na ARTE)

- Governação
	- Implementar uma estratégia de governação portável. Utilizar os metadados do delta como fonte de metadados para governação de forma a outras ferramentas (Purview, Unity, Amundsen, …) a puderem consumir.
		
		
		
		

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

