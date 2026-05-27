# Camada Bronze — Ingestão

A camada Bronze copia dados de fontes externas para o Lakehouse **sem transformar** o schema ou o conteúdo. É a landing zone fiel da fonte.

Para uma implementação concreta, ver:
- [Calamidade — Bronze](../calamidade/camada-bronze.md) — ingestão via Azure Blob Storage
- [Gapminder — Bronze](../gapminder/camada-bronze.md) — ingestão via REST API pública

---

## Responsabilidade

- Copiar ficheiros ou dados de fontes externas para o Lakehouse Bronze
- Preservar o ficheiro/dado original sem alterações de schema
- Registar metadados da ingestão (nome do ficheiro, timestamp)
- Sinalizar ao runner se existiram dados novos (`new_data`)

---

## Padrões de Ingestão

### Padrão: Consume-and-Delete (Azure Blob Storage)

Usado quando a fonte deposita ficheiros num contentor blob e o pipeline é o único consumidor:

```
1. Autenticar → obter SAS Token do Key Vault
2. Listar blobs no contentor
3. Para cada blob:
   a. Descarregar (streaming, sem carregar em memória)
   b. Fazer upload para o Lakehouse em chunks de 4 MB
   c. Apagar o blob na origem (só após upload confirmado)
4. Retornar { files_copied: [...] }
```

**Propriedade chave:** se o upload falhar, o blob NÃO é apagado e fica disponível para reprocessamento na próxima execução.

### Padrão: Pull via API

Usado quando a fonte expõe uma API REST e o pipeline vai buscar os dados activamente:

```
1. Descobrir o catálogo de dados disponíveis (endpoint de metadata)
2. Para cada item do catálogo:
   a. Fazer GET ao endpoint de dados
   b. Parsear a resposta (CSV, JSON, etc.)
   c. Escrever em tabela Delta no Lakehouse Bronze
3. Retornar { records_loaded: N }
```

---

## Upload por Chunks (Azure Data Lake Storage Gen2 REST API)

Para ficheiros grandes, o upload é feito em chunks de **4 MB** usando a ADLS Gen2 REST API:

1. `PUT` — cria o ficheiro vazio no destino (`?resource=file`)
2. `PATCH` em loop — anexa chunks (`?action=append&position=N`)
3. `PATCH` final — confirma a escrita (`?action=flush&position=total`)

Autenticação: Bearer Token via `mssparkutils.credentials.getToken("https://storage.azure.com/.default")`.

Este mecanismo está implementado na biblioteca `dap_lake_file_tools.py` ([ver Bibliotecas](bibliotecas-comuns.md#dap_lake_file_toolspy)).

---

## Resultado Devolvido ao Runner

O notebook bronze retorna sempre um JSON com o estado da execução:

```json
{ "status": "OK", "files_copied": [ { "name": "ficheiro.csv", "created_on": "..." } ] }
```

Se `files_copied` estiver vazio, o runner regista `new_data = False`. O avanço para silver e gold fica dependente da flag `run_withouth_new_data` de cada grupo.

---

## Tratamento de Erros

| `errorId` | Situação |
|-----------|----------|
| `0` | Sucesso |
| `10` | Falha de autenticação ou acesso à fonte |
| `20` | Falha durante a cópia ou eliminação de ficheiros |

Em produção (`running_local = False`), o notebook termina imediatamente com `notebookutils.notebook.exit(json)` em caso de erro.

---

[Visão Geral](visao-geral.md) · [Camada Silver →](camada-silver.md) · [Home](../Home.md)
