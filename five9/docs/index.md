# Documentação do Pipeline de Dados Five9

## Descrição

O projeto teve como principal ofensor a realização da busca de reports na Five9 através de APIs fornececida pela própria e a carga desses dados em nossos bancos de dados internos. Para isso no entanto, foi preciso o desenvolvimento de um código python que fizesse tanto a busca dos dados quanto a carga.

### Fluxo do ETL

```mermaid
flowchart LR
    subgraph pipeline[Pipeline]
        A[Manager Five9: Coleta de Dados] -->|Configura API Five9| B[Handler HTTPX: Realizada Chamada async]
        B[Handler HTTPX: Realizada Chamada async] -->|Extrai dados| C[Manager DBAPI: Salva no DBAPI]
        C[Manager DBAPI: Salva no DBAPI] -->|Cria Tabelas Stages| D[Manager Stage: Salva na Stage]
        D[Manager Stage: Salva na Stage] -->|Cria Tabelas Fatos| E[Manager DBDW: Salva no DBDW]
        E[Manager DBDW: Salva no DBDW] -->|Busca pelo Insert Recente| F[Manager ODS: Salva no ODS]
    end
```
