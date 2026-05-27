# Camada Silver — Normalização

A camada Silver lê dados do Bronze, normaliza o schema, faz cast de tipos, adiciona colunas de auditoria e escreve uma tabela Delta. A escrita é sempre **overwrite** — a tabela silver é substituída na íntegra a cada execução.

Para implementações concretas, ver:
- [Calamidade — Silver](../calamidade/camada-silver.md) — CCRD Habitação + BPF
- [Gapminder — Silver](../gapminder/camada-silver.md) — indicadores económicos

---

## Fluxo Padrão

```
1. Receber parâmetros via main_set
        ↓
2. Seleccionar ficheiro a processar (ver secção abaixo)
        ↓
3. Ler dados (CSV, Delta, API, etc.)
        ↓
4. Normalizar schema: cast de cada coluna segundo schema_map
        ↓
5. Limpar NULLs: substituir strings "NULL" (case-insensitive) por null real
        ↓
6. Adicionar colunas de auditoria
        ↓
7. df.write.format("delta").mode("overwrite").save(silver_path)
        ↓
8. Retornar { "status": "OK", "records_written": N }
```

---

## Selecção do Ficheiro a Processar

Quando a bronze são ficheiros com data no nome, a selecção do mais recente é automática:

### Modo 1 — Ficheiro explícito
Se `nome_ficheiro != ""`, o notebook usa esse nome directamente.

### Modo 2 — Selecção automática (modo normal de produção)
Se `nome_ficheiro == ""` (valor por defeito), o notebook chama `list_files_by_date_mask_startingDate`:

```python
matched, skipped = list_files_by_date_mask_startingDate(caminho_bronze, mask_ficheiro)
if len(matched) > 0:
    nome_ficheiro = matched[0]['name']    # ficheiro mais recente
    data_ficheiro = matched[0]['file_date']
```

A data é extraída do início do nome do ficheiro — ver [Fontes de Dados](fontes-de-dados.md#convenção-de-nomes-dos-ficheiros).

---

## Colunas de Auditoria

Adicionadas a todos os registos silver:

| Coluna | Tipo | Conteúdo |
|--------|------|----------|
| `origem` | `string` | Identificador da fonte (ex: `ccdr_centro`, `bpf`, `gapminder`) |
| `dt_ingest` | `timestamp` | `current_timestamp()` — momento da execução silver |
| `ficheiro_origem` | `string` | Nome do ficheiro CSV lido (quando aplicável) |
| `data_ficheiro` | `date` | Data extraída do nome do ficheiro |
| `row_number` | `int` | Número de linha sequencial (Window sobre `monotonically_increasing_id`) |

---

## Funções de Casting

Definidas localmente em cada notebook silver (ainda não extraídas para biblioteca):

| Função | Comportamento |
|--------|---------------|
| `cast_string` | `.cast("string")` simples |
| `cast_int` | Remove caracteres não numéricos, `.cast("int")` |
| `cast_long` | Remove caracteres não numéricos, `.cast("bigint")` |
| `cast_double` | Detecta formato PT (`1.234,56`), EN (`1,234.56`), inteiro com milhar, decimal simples. Devolve `null` se não reconhecer |
| `cast_timestamp` | Tenta múltiplos formatos em sequência. Suporta datas Excel (número de série de 5 dígitos) via `allow_excel_serial` |
| `cast_date` | Variante do timestamp, devolve `DateType` |

---

## Tratamento de Erros

Existem dois padrões em uso — o padrão novo é o de referência.

### Padrão Novo — `handleError` (referência)

```python
def handleError(step_number, e, running_local, results=[]):
    if not running_local:
        result = {"status": "ERROR", "errorState": json.dumps(e.args), "errorId": step_number}
        notebookutils.notebook.exit(json.dumps(result))   # saída imediata em produção
    return e.args, step_number

# Uso em cada passo:
if errorId == 0:
    try:
        df = normalizar_colunas(df, schema_map)
    except Exception as e:
        errorState, errorId = handleError(40, e, running_local)
```

**Vantagem:** em produção, o notebook termina no primeiro erro sem executar código subsequente.

### Padrão Antigo — verificação inline

Verifica `errorId` antes de cada passo e acumula o erro, terminando apenas no final do notebook. Menos eficiente e mais difícil de depurar. Notebooks com este padrão devem ser migrados para `handleError`.

---

## Códigos de Erro Comuns

| `errorId` | Situação |
|-----------|----------|
| `0` | Sucesso |
| `10` | Falha na configuração inicial / parâmetros |
| `20` | Ficheiro não encontrado no bronze |
| `30`/`31` | Falha na leitura do CSV |
| `40` | Falha na normalização de colunas |
| `50` | Falha na limpeza de NULLs / colunas derivadas |
| `60`–`80` | Falha na escrita Delta |

---

[← Camada Bronze](camada-bronze.md) · [Camada Gold →](camada-gold.md) · [Home](../Home.md)
