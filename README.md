# Documentação Completa - Workspace Fabric

## 📊 Diagrama da Arquitetura

![Arquitetura Medallion](arquitetura_medallion.png)

---

## 🔄 Fluxo de Dados

```
Fonte → Bronze → Silver → Gold → Semantic Model → Report
```

---

## 📓 Documentação dos Notebooks

### Bronze
#### `WorldDataBank_nb_bronze_facts`
- **Input:** fontes externas (ficheiros, APIs)
- **Output:** tabelas raw no Lakehouse Bronze

#### `WorldDataBank_nb_localizacoes`
- **Input:** dados geográficos
- **Output:** tabela de localização base

---

### Silver
#### `WorldDataBank_nb_silver_dims`
- **Input:** dados Bronze
- **Output:** tabelas dimensão (país, região, etc.)

#### `WorldDataBank_nb_silver_facts`
- **Input:** dados Bronze
- **Output:** factos normalizados

#### `WorldDataBank_nb_silver_set_import`
- **Input:** datasets intermédios
- **Output:** datasets prontos para integração na camada Gold

---

### Gold
- Dados agregados e modelados
- Preparados para consumo do Semantic Model

---

## ✅ Resumo

Arquitetura baseada em Medallion com separação clara de responsabilidades e pipeline estruturado.

