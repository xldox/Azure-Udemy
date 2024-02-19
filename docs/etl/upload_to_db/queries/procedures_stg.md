# Consultas SQL para o Banco de Dados STAGE

Aqui estão as consultas que são rodadas de forma interna por meio de procedures. Essas *queries* consultam os dados que estão em tabelas auxiliares localizadas no mesmo banco de dados. A partir dessas tabelas realiza-se alguns tratamentos tais como: alteração dos nomes das colunas, ajuste dos tipos, tratamentos de nulos e combinações entre colunas para criações de outras novas. Abaixo estão todas as *queries* das procedures que geram as tabelas stages.

## Procedures SQL para chamadas de mídia voz

Neste bloco é visto todas as procedures para o tipo de mídia voz.

### Stage Chamadas Ativas

```sql
CREATE PROCEDURE five9.proc_stg_chamadas_ativas_voz
AS
BEGIN

TRUNCATE TABLE STAGE.five9.stg_chamadas_ativas_voz_v2;

WITH cte AS
(
    SELECT
        [CALL ID] AS id_chamada
         , CAST(CAST(data_ligacao AS DATETIME2) AS DATETIME) AS data_inicio_chamada
		 , IIF(CAMPAIGN = '[Nenhum]' OR CAMPAIGN = '[None]', NULL, CAMPAIGN) AS campanha
		 , IIF(AGENT = '[Nenhum]' OR AGENT = '[None]', null, LOWER(AGENT)) AS agente
		 , TRIM(DISPOSITION) AS resultado
		 , IIF([CUSTOMER NAME] = 'nan' OR [CUSTOMER NAME] LIKE '%[1-9]%', NULL, TRIM(UPPER([CUSTOMER NAME]))) AS cliente
         , IIF([CPF-CNPJ] LIKE '%[a-z]%' OR LEN([CPF-CNPJ]) < 9 OR LEN([CPF-CNPJ]) > 16, NULL, REPLACE(REPLACE(REPLACE(REPLACE([CPF-CNPJ], '.', ''), '-', ''), '/', ''), '.0', '')) AS cpf_cnpj
         , IIF(Contrato = 'nan' OR Contrato LIKE '%[a-z]%', NULL, TRIM(REPLACE(Contrato, '.0', ''))) AS contrato
         , [CALL TYPE] AS tipo_chamada
         , CASE
             WHEN LEN(DNIS) > 11 THEN RIGHT(DNIS, LEN(DNIS) - (LEN(DNIS) - 11))
             WHEN DNIS LIKE '+55%' THEN REPLACE(DNIS, '+55', '')
             ELSE DNIS
           END AS destino
         , IIF(TRANSFERS = 'nan', 0, REPLACE(TRANSFERS, '.0', '')) AS transferida
         , IIF(ABANDONED = 'nan', 0, REPLACE(ABANDONED, '.0', '')) AS abandonada
         , IIF([TALK TIME] = 'nan', null, [TALK TIME]) AS tempo_conversa
         , CASE
             WHEN UPPER(company) = 'SUMICITY' THEN 'SUMICITY'
             WHEN UPPER(company) = 'CLICK' THEN 'CLICK'
             WHEN UPPER(company) = 'UNIVOX' THEN 'UNIVOX'
             WHEN UPPER(company) LIKE 'GIGA%' THEN 'GIGA+ FIBRA'
             ELSE
                 CASE
                     WHEN UPPER([CUSTOMER NAME]) LIKE '%SUMICITY%' THEN 'SUMICITY'
                     WHEN UPPER([CUSTOMER NAME]) LIKE '%GIGA%' THEN 'GIGA+ FIBRA'
                     WHEN UPPER([CUSTOMER NAME]) LIKE '%CLICK%' THEN 'CLICK'
                     WHEN UPPER([CUSTOMER NAME]) LIKE '%UNIVOX%' THEN 'UNIVOX'
                     ELSE
                         CASE
                             WHEN UPPER(CAMPAIGN) LIKE '%SUMICITY%' THEN 'SUMICITY'
                             WHEN UPPER(CAMPAIGN) LIKE 'LIGUE%' THEN 'LIGUE'
                             WHEN UPPER(CAMPAIGN) LIKE 'NIU%' THEN 'NIU'
                             WHEN UPPER(CAMPAIGN) LIKE '%MOB%' THEN 'MOBWIRE'
                             ELSE 'VIP'
                         END
                 END
           END AS marca
         , CASE
             WHEN UPPER(company) IN ('SUMICITY', 'GIGA+', 'GIGA+ FIBRA', 'CLICK', 'UNIVOX') THEN 'SUMICITY'
             ELSE
                CASE
                    WHEN UPPER([CUSTOMER NAME]) LIKE '%/ SUMICITY%' OR UPPER([CUSTOMER NAME]) LIKE '%/ GIGA%' OR UPPER([CUSTOMER NAME]) LIKE '%/ CLICK%' OR UPPER([CUSTOMER NAME]) LIKE '%/ UNIVOX%' THEN 'SUMICITY'
                    ELSE
                        CASE
                            WHEN UPPER(CAMPAIGN) LIKE '%SUMICITY%' THEN 'SUMICITY'
                            WHEN UPPER(CAMPAIGN) LIKE '%MOB%' THEN 'MOB'
                            ELSE 'VIP'
                        END
                END
           END polo
        , data_extracao
		, IIF(ID_Fatura = 'nan', NULL, REPLACE(ID_Fatura, '.0', '')) AS id_fatura

    FROM STAGE.five9.chamadas_ativas_voz
)
INSERT INTO STAGE.five9.stg_chamadas_ativas_voz_v2
SELECT
    cte.id_chamada, cte.data_inicio_chamada, cte.campanha, cte.agente, cte.resultado, cte.cliente
     , CASE
         WHEN LEN(cte.cpf_cnpj) < 9 OR LEN(cte.cpf_cnpj) > 14 THEN NULL
         WHEN LEN(cte.cpf_cnpj) > 11 THEN REPLICATE('0', 14 - LEN(cte.cpf_cnpj)) + RTRIM(cte.cpf_cnpj)
         ELSE REPLICATE('0', 11 - LEN(cte.cpf_cnpj)) + RTRIM(cte.cpf_cnpj)
       END AS cpf_cnpj
     , cte.contrato, cte.tipo_chamada, cte.destino, cte.transferida, cte.abandonada, cte.tempo_conversa
     , cte.marca, cte.data_extracao, cte.id_fatura, cte.polo

FROM cte

END
```

### Stage Chamadas Recebidas

```sql
CREATE PROCEDURE five9.proc_stg_chamadas_recebidas_voz
AS
BEGIN

TRUNCATE TABLE STAGE.five9.stg_chamadas_recebidas_voz_v2;

WITH cte AS
(
    SELECT
       [CALL ID] AS id_chamada
        , CAST(CAST(data_ligacao AS DATETIME2) AS DATETIME) AS data_inicio_chamada
        , TRIM(CAMPAIGN) AS campanha
        , IIF(SKILL = '[Nenhum]' OR SKILL = '[None]', null, SKILL) AS servico
        , IIF(AGENT = '[Nenhum]' OR AGENT = '[None]', null, AGENT) AS agente
        , TRIM(DISPOSITION) AS resultado
	    , IIF([DISPOSITION GROUP A] = 'nan', null, TRIM([DISPOSITION GROUP A])) AS classificacao_grupo_a
	    , IIF([DISPOSITION GROUP B] = 'nan', null, TRIM([DISPOSITION GROUP B])) AS classificacao_grupo_b
	    , IIF([DISPOSITION GROUP C] = 'nan', null, TRIM([DISPOSITION GROUP C])) AS classificacao_grupo_c
	    , IIF([Alloha.Retenção] = 'nan', NULL, TRIM([Alloha.Retenção]))	AS motivo_retencao_ura
        , IIF(LOWER([Custom.CPF]) IN ('undefined', 'nan') OR [Custom.CPF] LIKE '%*%' OR [Custom.CPF] LIKE '000%' OR LEN([Custom.CPF]) < 9 OR LEN([Custom.CPF]) > 16, NULL, REPLACE([Custom.CPF], '.0', '')) AS cpf_cnpj
		, IIF(LOWER([Alloha.CPF_CNPJ]) IN ('undefined', 'nan') OR [Alloha.CPF_CNPJ] LIKE '%*%' OR [Alloha.CPF_CNPJ] LIKE '000%' OR LEN([Alloha.CPF_CNPJ]) < 9 OR LEN([Alloha.CPF_CNPJ]) > 16, NULL, REPLACE([Alloha.CPF_CNPJ], '.0', '')) AS cpf_cnpj_alloha
        , IIF([Custom.Contrato] LIKE '%[a-z]%' OR [Custom.Contrato] = 'nan', NULL, REPLACE([Custom.Contrato], '.0', '')) AS contrato
        , [CALL TYPE] AS tipo_chamada
        , CASE
            WHEN LOWER(ANI) LIKE '%[a-z]%' OR LEN(ANI) < 8 THEN NULL
            WHEN LEN(ANI) > 11 THEN RIGHT(ani, LEN(ANI) - (LEN(ANI) - 11))
            WHEN ANI LIKE '+55%' THEN REPLACE(ANI, '+55', '')
            ELSE ANI
          END AS origem
        , CASE
            WHEN DNIS LIKE '+55%' THEN REPLACE(DNIS, '+55', '')
            WHEN DNIS LIKE '55%' THEN RIGHT(DNIS, LEN(DNIS) - 2)
            ELSE DNIS
          END AS destino
        , IIF(TRANSFERS = 'nan' OR TRANSFERS IS NULL, 0, REPLACE(TRANSFERS, '.0', '')) AS transferida
        , IIF(ABANDONED = 'nan' OR ABANDONED IS NULL, 0, REPLACE(ABANDONED, '.0', '')) AS abandonada
        , IIF([Custom.Massiva] = 'nan', NULL, [Custom.Massiva]) AS massiva
        , IIF([Custom.Cidade] IN ('OK', 'Unauthorized', 'undefined', 'nan') OR [Custom.Cidade] LIKE '%[1-9]%', NULL, TRIM(UPPER([Custom.Cidade]))) AS cidade
        , IIF([Custom.Bairro] IN ('undefined', 'nan'), NULL, TRIM(UPPER([Custom.Bairro]))) AS bairro
        , IIF(LOWER([Custom.Endereço]) = 'nan', NULL, TRIM(UPPER([Custom.Endereço]))) AS endereco
        , IIF([Pesquisa.Pergunta01] LIKE '%[0-9]%', REPLACE([Pesquisa.Pergunta01], '.0', ''), IIF([Alloha.Pergunta1_PesquisaAgente] LIKE '%[0-9]%', REPLACE([Alloha.Pergunta1_PesquisaAgente], '.0', ''), NULL)) AS pergunta_satisfacao_1
        , IIF([Pesquisa.Pergunta02] LIKE '%[0-9]%', REPLACE([Pesquisa.Pergunta02], '.0', ''), IIF([Alloha.Pergunta2_PesquisaAgente] LIKE '%[0-9]%', REPLACE([Alloha.Pergunta2_PesquisaAgente], '.0', ''), NULL)) AS pergunta_satisfacao_2
        , IIF([Pesquisa.Pergunta03] LIKE '%[0-9]%', REPLACE([Pesquisa.Pergunta03], '.0', ''), IIF([Alloha.Pergunta3_PesquisaAgente] LIKE '%[0-9]%', REPLACE([Alloha.Pergunta3_PesquisaAgente], '.0', ''), NULL)) AS pergunta_satisfacao_3
        , CASE
            WHEN DNIS LIKE '%2220500101' THEN 'SUMICITY'
            WHEN [Custom.EmpresaMarca] IS NOT NULL AND [Custom.EmpresaMarca] <> 'nan' THEN
                CASE
                    WHEN [Custom.EmpresaMarca] LIKE '%SUMI%' THEN 'SUMICITY'
                    WHEN [Custom.EmpresaMarca] LIKE '%GIG%' THEN 'GIGA+ FIBRA'
                    WHEN [Custom.EmpresaMarca] LIKE '%CLI%' THEN 'CLICK'
                    WHEN [Custom.EmpresaMarca] LIKE '%UNI%' THEN 'UNIVOX'
                    WHEN [Custom.EmpresaMarca] LIKE '%VI%' THEN 'VIP'
                    WHEN [Custom.EmpresaMarca] LIKE '%LIG%' THEN 'LIGUE'
                    WHEN [Custom.EmpresaMarca] LIKE '%NI%' THEN 'NIU'
                    WHEN [Custom.EmpresaMarca] LIKE '%MO%' THEN 'MOBWIRE'
                END
            WHEN [Alloha.Polo] IS NOT NULL AND [Alloha.Polo] <> 'nan' THEN
                CASE
                    WHEN [Alloha.Polo] LIKE '%SUMI%' THEN 'SUMICITY'
                    WHEN [Alloha.Polo] LIKE '%GIG%' THEN 'GIGA+ FIBRA'
                    WHEN [Alloha.Polo] LIKE '%CLI%' THEN 'CLICK'
                    WHEN [Alloha.Polo] LIKE '%UNI%' THEN 'UNIVOX'
                    WHEN [Alloha.Polo] LIKE '%VI%' THEN 'VIP'
                    WHEN [Alloha.Polo] LIKE '%LIG%' THEN 'LIGUE'
                    WHEN [Alloha.Polo] LIKE '%NI%' THEN 'NIU'
                    WHEN [Alloha.Polo] LIKE '%MO%' THEN 'MOBWIRE'
                END
            ELSE
                CASE
                    WHEN CAMPAIGN = 'PRINCIPAL' THEN 'VIP'
                    WHEN CAMPAIGN LIKE 'LIG%' THEN 'LIGUE'
                    WHEN CAMPAIGN LIKE 'NIU%' THEN 'NIU'
                    WHEN CAMPAIGN LIKE '%MOB%' THEN 'MOBWIRE'
                END
          END marca
        , CASE
            WHEN CAMPAIGN LIKE '%SUMICITY%' THEN 'SUMICITY'
            WHEN CAMPAIGN LIKE '%MOB%' OR [Custom.EmpresaMarca] LIKE '%MOB%' OR [Alloha.Polo] LIKE '%MOB%' THEN 'MOB'
            ELSE 'VIP'
          END polo
        , data_extracao

    FROM STAGE.five9.chamadas_recebidas_voz
)
INSERT INTO STAGE.five9.stg_chamadas_recebidas_voz_v2
SELECT
    id_chamada, data_inicio_chamada, campanha, servico, agente
     , resultado, classificacao_grupo_a, classificacao_grupo_b, classificacao_grupo_c, motivo_retencao_ura
     , IIF(cpf_cnpj IS NULL, cpf_cnpj_alloha, cpf_cnpj) AS cpf_cnpj
	 , contrato, tipo_chamada, origem, destino, transferida, abandonada, massiva
     , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(cidade), 'Á', 'A'), 'Ã', 'A'), 'Â', 'A'), 'À', 'A'), 'É', 'E'), 'Ê', 'E'), 'Í', 'I'), 'Ú', 'U'), 'CAMPO MOUR?A', 'CAMPO MOURAO') AS cidade
     , bairro, endereco
     , pergunta_satisfacao_1, pergunta_satisfacao_2, pergunta_satisfacao_3
     , marca
     , data_extracao, cte.polo

FROM cte

END
```

### Stage TMA

```sql
CREATE PROCEDURE five9.proc_stg_tma
AS
BEGIN

TRUNCATE TABLE STAGE.five9.stg_tma_v2;

INSERT INTO STAGE.five9.stg_tma_v2
SELECT
    [CALL ID] AS id_chamada
     , CAST(CAST(data_ligacao AS DATETIME2) AS DATETIME) AS data_inicio_chamada
     , CAMPAIGN AS campanha
     , [CALLED PARTY] AS segmento
     , [SEGMENT TYPE] AS tipo_segmento
     , [CALL TIME] AS tempo_chamada
     , IIF([TALK TIME] = 'nan', NULL, [TALK TIME]) AS tempo_conversa
     , IIF([QUEUE WAIT TIME] = 'nan', NULL, [QUEUE WAIT TIME]) AS tempo_espera_fila
     , IIF([RING TIME] = 'nan', NULL, [RING TIME]) AS tempo_toque
     , [AFTER CALL WORK TIME] AS tempo_pos_atendimento

FROM STAGE.five9.tma

END
```

### Stage TMA Transferidas

```sql
CREATE PROCEDURE five9.proc_stg_tma_transferidas
AS
BEGIN

TRUNCATE TABLE STAGE.five9.stg_tma_transferidas_v2;

SELECT
    [CALL ID] AS id_chamada
     , CAST(CAST(data_ligacao AS DATETIME2) AS DATETIME2) AS data_ligacao
     , CAMPAIGN AS campanha
     , [CALLED PARTY] AS segmento
     , [SEGMENT TYPE] AS tipo_segmento
     , [CALL TIME] AS tempo_chamada
     , IIF([TALK TIME] = 'nan', NULL, [TALK TIME]) AS tempo_conversa
     , IIF([QUEUE WAIT TIME] = 'nan', NULL, [QUEUE WAIT TIME]) AS tempo_espera_fila
     , IIF([RING TIME] = 'nan', NULL, [RING TIME]) AS tempo_toque
     , [AFTER CALL WORK TIME] AS tempo_pos_atendimento

INTO #base_tma

FROM STAGE.five9.tma
;

-- Separando linhas de transferencia e obtendo dados de linhas anteriores
SELECT
    id_chamada
    , CASE
        WHEN LAG(id_chamada) OVER (PARTITION BY id_chamada ORDER BY data_ligacao ASC) = id_chamada AND tipo_segmento IN ('Transfer to Skill', 'Warm Transfer', 'Retrieve from Park', 'Transfer to Agent')
            THEN LAG(data_ligacao) OVER (PARTITION BY id_chamada  ORDER BY data_ligacao ASC)
        ELSE NULL
      END AS hora_chamada
    , CASE
        WHEN tipo_segmento IN ('Transfer to Skill', 'Warm Transfer', 'Retrieve from Park', 'Transfer to Agent')
            THEN data_ligacao
        ELSE NULL
      END AS hora_atendimento
    , CASE
        WHEN LAG(id_chamada) OVER (PARTITION BY id_chamada ORDER BY data_ligacao ASC) = id_chamada AND tipo_segmento = 'Transfer to Skill'
            THEN LAG(segmento) OVER (PARTITION BY id_chamada  ORDER BY data_ligacao ASC)
        ELSE NULL
      END AS competencia
    , segmento
    , tipo_segmento
    , tempo_chamada
    , tempo_conversa
    , CASE
        WHEN LAG(id_chamada) OVER (PARTITION BY id_chamada ORDER BY data_ligacao ASC) = id_chamada AND tipo_segmento = 'Transfer to Skill'
            THEN LAG(tempo_espera_fila) OVER (PARTITION BY id_chamada  ORDER BY data_ligacao ASC)
        ELSE NULL
      END AS tempo_espera_fila
    , tempo_toque
    , tempo_pos_atendimento

INTO #base_processada_tma_1

FROM #base_tma

WHERE tipo_segmento IN ('Transfer to Skill', 'Skill Transfer', 'Warm Transfer', 'Retrieve from Park', 'Transfer to Agent')

ORDER BY id_chamada
;

-- 1º ajuste das competências que possuem agente ou valor nulo
SELECT
    id_chamada
    , hora_chamada
    , hora_atendimento
    , CASE
        WHEN competencia LIKE '%@%' OR competencia IS NULL THEN
            CASE WHEN LAG(id_chamada) OVER (PARTITION BY id_chamada ORDER BY hora_chamada ASC) = id_chamada THEN LAG(competencia) OVER (PARTITION BY id_chamada  ORDER BY hora_chamada ASC) END
        ELSE competencia
      END AS competencia
    , segmento
    , tipo_segmento
    , tempo_chamada
    , tempo_conversa
    , tempo_espera_fila
    , tempo_toque
    , tempo_pos_atendimento

INTO #base_processada_tma_2

FROM #base_processada_tma_1
;

-- 2º ajuste das competências que possuem agente ou valor nulo
SELECT
	id_chamada
    , hora_chamada
    , hora_atendimento
	, CASE
	    WHEN competencia LIKE '%@%' OR competencia IS NULL THEN
	        CASE WHEN LAG(id_chamada) OVER (PARTITION BY id_chamada ORDER BY hora_chamada ASC) = id_chamada THEN LAG(competencia) OVER (PARTITION BY id_chamada  ORDER BY hora_chamada ASC) END
	    ELSE competencia
	  END AS competencia
	, segmento
	, tipo_segmento
    , tempo_chamada
	, tempo_conversa
    , tempo_espera_fila
    , tempo_toque
    , tempo_pos_atendimento

INTO #base_processada_tma_3

FROM #base_processada_tma_2
;

-- 3º ajuste das competências que possuem agente ou valor nulo
SELECT
	id_chamada
    , hora_chamada
    , hora_atendimento
	, CASE
	    WHEN competencia LIKE '%@%' OR competencia IS NULL THEN
	        CASE WHEN LAG(id_chamada) OVER (PARTITION BY id_chamada ORDER BY hora_chamada ASC) = id_chamada THEN LAG(competencia) OVER (PARTITION BY id_chamada  ORDER BY hora_chamada ASC) END
	    ELSE competencia
	  END AS competencia
	, segmento
	, tipo_segmento
    , tempo_chamada
	, tempo_conversa
    , tempo_espera_fila
    , tempo_toque
    , tempo_pos_atendimento

INTO #base_processada_tma_4

FROM #base_processada_tma_3
;

-- 4º ajuste das competências que possuem agente ou valor nulo
SELECT
	id_chamada
    , hora_chamada
    , hora_atendimento
	, CASE
	    WHEN competencia LIKE '%@%' OR competencia IS NULL THEN
	        CASE WHEN LAG(id_chamada) OVER (PARTITION BY id_chamada ORDER BY hora_chamada ASC) = id_chamada THEN LAG(competencia) OVER (PARTITION BY id_chamada  ORDER BY hora_chamada ASC) END
	    ELSE competencia
	  END AS competencia
	, segmento
	, tipo_segmento
    , tempo_chamada
	, tempo_conversa
    , tempo_espera_fila
    , tempo_toque
    , tempo_pos_atendimento

INTO #base_processada_tma_5

FROM #base_processada_tma_4
;

-- Criando indice das transferencias e Inserindo na tabela Stg
INSERT INTO STAGE.five9.stg_tma_transferidas_v2
SELECT
	ROW_NUMBER() OVER (PARTITION BY id_chamada ORDER BY hora_chamada) AS indice
	, id_chamada
    , hora_chamada AS data_inicio_chamada
    , hora_atendimento AS data_inicio_atendimento
	, competencia AS servico
	, CASE WHEN LEAD(id_chamada) OVER (PARTITION BY id_chamada ORDER BY hora_chamada ASC) = id_chamada THEN LEAD(competencia) OVER (PARTITION BY id_chamada  ORDER BY hora_chamada ASC) END AS destino_transferencia
	, segmento
	, tipo_segmento
    , tempo_chamada
	, tempo_conversa
    , tempo_espera_fila
    , tempo_toque
    , tempo_pos_atendimento

FROM #base_processada_tma_5

WHERE tipo_segmento <> 'Skill Transfer' AND segmento LIKE '%@%'

ORDER BY id_chamada, hora_chamada
;


DROP TABLE #base_tma;
DROP TABLE #base_processada_tma_1;
DROP TABLE #base_processada_tma_2;
DROP TABLE #base_processada_tma_3;
DROP TABLE #base_processada_tma_4;
DROP TABLE #base_processada_tma_5;

END
```

### Stage Disconnect First

```sql
CREATE PROCEDURE five9.proc_stg_disconnect_first_voz
AS
BEGIN

TRUNCATE TABLE STAGE.five9.stg_disconnect_first_voz_v2;

INSERT STAGE.five9.stg_disconnect_first_voz_v2
SELECT
    [CALL ID] AS id_chamada
     , CAST(CAST(data_ligacao AS DATETIME2) AS DATETIME) AS data_inicio_chamada
     , [AGENT EMAIL] AS email_agente
     , [AGENT DISCONNECTS FIRST] AS agente_desconecta_primeiro

FROM STAGE.five9.disconnect_first_voz

END
```

## Procedures SQL para chamadas de mídia chat

Neste bloco é visto todas as procedures para o tipo de mídia chat.

### Stage Chamadas Recebidas

```sql
CREATE PROCEDURE five9.proc_stg_chamadas_chat
AS
BEGIN

TRUNCATE TABLE STAGE.five9.stg_chamadas_chat_v2;

WITH cte_tratamentos_preliminares AS
(
    SELECT
        chat.[SESSION GUID] AS id_chamada
        , CAST(CAST(chat.data_ligacao AS DATETIME2) AS DATETIME) AS data_inicio_chamada
        , TRIM(chat.CAMPAIGN) AS campanha
        , IIF(chat.AGENT = '[Nenhum]' OR chat.AGENT = '[None]' OR chat.AGENT = 'nan', null, chat.AGENT) AS agente
        , chat.[MEDIA TYPE] AS tipo_midia
        , chat.[MEDIA SUBTYPE] AS subtipo_midia
        , TRIM(chat.DISPOSITION) AS resultado
        , IIF(base_var.[Alloha.Retenção] = 'nan', null, TRIM(base_var.[Alloha.Retenção])) AS motivo_retencao_ura
        , IIF(chat.[CUSTOMER NAME] = 'nan', null, chat.[CUSTOMER NAME]) AS cliente
        , CASE
            WHEN chat.[FROM ADDRESS] = 'nan' THEN NULL
            WHEN chat.[FROM ADDRESS] LIKE 'WP%' THEN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LEFT(chat.[FROM ADDRESS], CHARINDEX('@', chat.[FROM ADDRESS])), 'WP_55', ''), 'WP_', ''), 'WP-55', ''), 'WP-', ''), '@', '')
            WHEN chat.[FROM ADDRESS] LIKE 'WA_5%' THEN REPLACE(REPLACE(LEFT(chat.[FROM ADDRESS], CHARINDEX('@', chat.[FROM ADDRESS])), 'WA_55', ''), '@', '')
            ELSE chat.[FROM ADDRESS]
          END AS origem_do_endereco
        , CASE
            WHEN base_var.[Alloha.NumeroChat] = 'nan' THEN NULL
            WHEN LOWER(base_var.[Alloha.NumeroChat]) LIKE '%[a-z]%' THEN TRIM(base_var.[Alloha.NumeroChat])
            ELSE TRIM(REPLACE(base_var.[Alloha.NumeroChat], '-', ''))
          END AS origem_var
        , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(base_var.[Custom.CPF], ' ', ''), ',', ''), '-', ''), '/', ''), '_', ''), '.', ''), '?', ''), ')', ''), '(', ''), '.0', '') AS cpf_cnpj
        , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(base_var.[Alloha.CPF_CNPJ]), ' ', ''), ':', ''), ',', ''), '_', ''), '''', ''), 'CPF', ''), 'CNPJ', ''), ')', ''), '(', ''), '-', ''), '/', ''), '.', ''), '?', ''), '.0', '') AS alloha_cpf_cnpj
        , IIF(base_var.[Custom.Contrato] = 'nan' OR lower(base_var.[Custom.Contrato]) LIKE '%[a-z]%', null , REPLACE(base_var.[Custom.Contrato], '.0', '')) AS contrato
        , IIF(base_var.[Alloha.Contrato] = 'nan' OR lower(base_var.[Alloha.Contrato]) LIKE '%[a-z]%', null, REPLACE(base_var.[Alloha.Contrato], '.0', '')) AS alloha_contrato
        , IIF(base_var.[Custom.Cidade] = 'nan', null, TRIM(base_var.[Custom.Cidade])) AS cidade
        , IIF(base_var.[Custom.CEP] = 'nan', null, REPLACE(base_var.[Custom.CEP], '.0', '')) AS cep
        , IIF(base_var.[Custom.Bairro] = 'nan', null, TRIM(base_var.[Custom.Bairro])) AS bairro
		, IIF(base_var.[Alloha.Pergunta1_PesquisaAgente] = 'nan', NULL, base_var.[Alloha.Pergunta1_PesquisaAgente]) AS pergunta_satisfacao_1
		, IIF(base_var.[Alloha.Pergunta2_PesquisaAgente] = 'nan', NULL, base_var.[Alloha.Pergunta2_PesquisaAgente]) AS pergunta_satisfacao_2
		, IIF(base_var.[Alloha.Pergunta3_PesquisaAgente] = 'nan', NULL, base_var.[Alloha.Pergunta3_PesquisaAgente]) AS pergunta_satisfacao_3
        , chat.[INTERACTION TIME] AS tempo_interacao
        , chat.[CHAT TIME] AS tempo_conversa
        , chat.[QUEUE TIME] AS tempo_espera_fila
        , chat.[AFTER CHAT WORK] AS apos_horario_trabalho
        , IIF(chat.[TRANSFERRED FROM] = '[Nenhum]' OR chat.[TRANSFERRED FROM] = '[None]' OR chat.[TRANSFERRED FROM] = 'nan', null, chat.[TRANSFERRED FROM]) AS transferida_de
        , IIF(chat.[TRANSFERRED TO] = '[Nenhum]' OR chat.[TRANSFERRED TO] = '[None]' OR chat.[TRANSFERRED TO] = 'nan', null, chat.[TRANSFERRED TO]) AS transferida_para
        , chat.TRANSFERS AS transferencias
        , chat.data_extracao AS data_extracao
        , CASE
            WHEN UPPER(base_var.[Custom.EmpresaMarca]) LIKE '%SUMI%' THEN 'SUMICITY'
            WHEN UPPER(base_var.[Custom.EmpresaMarca]) LIKE '%GIG%' THEN 'GIGA+ FIBRA'
            WHEN UPPER(base_var.[Custom.EmpresaMarca]) LIKE '%CLIC%' THEN 'CLICK'
            WHEN UPPER(base_var.[Custom.EmpresaMarca]) LIKE '%UNIV%' THEN 'UNIVOX'
            WHEN UPPER(base_var.[Custom.EmpresaMarca]) LIKE '%VIP%' THEN 'VIP'
            WHEN UPPER(base_var.[Custom.EmpresaMarca]) LIKE '%LIGUE%' THEN 'LIGUE'
            WHEN UPPER(base_var.[Custom.EmpresaMarca]) LIKE '%NIU%' THEN 'NIU'
            WHEN UPPER(base_var.[Custom.EmpresaMarca]) LIKE '%MOB%' THEN 'MOBWIRE'
            ELSE NULL
        END AS marca_padrao
        , CASE
            WHEN UPPER(base_var.[Alloha.Marca]) LIKE '%SUMI%' THEN 'SUMICITY'
            WHEN UPPER(base_var.[Alloha.Marca]) LIKE '%GIG%' THEN 'GIGA+ FIBRA'
            WHEN UPPER(base_var.[Alloha.Marca]) LIKE '%CLIC%' THEN 'CLICK'
            WHEN UPPER(base_var.[Alloha.Marca]) LIKE '%UNIV%' THEN 'UNIVOX'
            WHEN UPPER(base_var.[Alloha.Marca]) LIKE '%VIP%' THEN 'VIP'
            WHEN UPPER(base_var.[Alloha.Marca]) LIKE '%LIGUE%' THEN 'LIGUE'
            WHEN UPPER(base_var.[Alloha.Marca]) LIKE '%NIU%' THEN 'NIU'
            WHEN UPPER(base_var.[Alloha.Marca]) LIKE '%MOB%' THEN 'MOBWIRE'
            ELSE NULL
          END AS marca_alloha
        , CASE
            WHEN UPPER(chat.[TRANSFERRED TO]) LIKE '%SUMI%' THEN 'SUMICITY'
            WHEN UPPER(chat.[TRANSFERRED TO]) LIKE '%GIG%' THEN 'GIGA+ FIBRA'
            WHEN UPPER(chat.[TRANSFERRED TO]) LIKE '%CLIC%' THEN 'CLICK'
            WHEN UPPER(chat.[TRANSFERRED TO]) LIKE '%UNIV%' THEN 'UNIVOX'
            WHEN UPPER(chat.[TRANSFERRED TO]) LIKE '%VIP%' THEN 'VIP'
            WHEN UPPER(chat.[TRANSFERRED TO]) LIKE '%LIGUE%' THEN 'LIGUE'
            WHEN UPPER(chat.[TRANSFERRED TO]) LIKE '%NIU%' THEN 'NIU'
            WHEN UPPER(chat.[TRANSFERRED TO]) LIKE '%MOB%' OR UPPER(chat.[TRANSFERRED TO]) LIKE '%WIRE%' THEN 'MOBWIRE'
            ELSE
                CASE
                    WHEN UPPER(chat.CAMPAIGN) LIKE '%SUMI%' THEN 'SUMICITY'
                    WHEN UPPER(chat.CAMPAIGN) LIKE '%GIG%' THEN 'GIGA+ FIBRA'
                    WHEN UPPER(chat.CAMPAIGN) LIKE '%CLIC%' THEN 'CLICK'
                    WHEN UPPER(chat.CAMPAIGN) LIKE '%UNIV%' OR chat.AGENT LIKE '%univox%' THEN 'UNIVOX'
                    WHEN UPPER(chat.CAMPAIGN) LIKE '%MOB%' OR UPPER(chat.CAMPAIGN) LIKE '%WIRE%' OR chat.AGENT LIKE '%mobwir%' THEN 'MOBWIRE'
                    WHEN UPPER(chat.CAMPAIGN) LIKE '%LIGUE%' OR chat.AGENT LIKE '%ligue%' THEN 'LIGUE'
                    WHEN UPPER(chat.CAMPAIGN) LIKE '%NIU%' OR chat.AGENT LIKE '%niut%' OR chat.AGENT LIKE '%niuf%' THEN 'NIU'
                    ELSE 'VIP'
                END
          END AS marca_outros


    FROM STAGE.five9.chamadas_chat chat
    LEFT JOIN STAGE.five9.base_variaveis_chat base_var ON chat.[SESSION GUID] = base_var.[Custom.Guia da Sessão]

	WHERE (chat.CAMPAIGN NOT LIKE 'teste%' OR chat.CAMPAIGN NOT LIKE 'alloha')
),
cte_tratamento_final AS
(
    SELECT
        id_chamada, data_inicio_chamada, campanha, agente, tipo_midia, subtipo_midia, resultado, motivo_retencao_ura, cliente
         , CASE
             WHEN origem_var IS NOT NULL THEN
                CASE
                     WHEN origem_var LIKE '%[a-z]%' AND origem_do_endereco LIKE '%@%' THEN origem_do_endereco
                     WHEN (origem_var LIKE '%[0-9]%' AND origem_var NOT LIKE '%[a-z]%') OR
                          origem_var LIKE '%@%' THEN origem_var
                END
             WHEN (origem_do_endereco LIKE '%[0-9]%' AND origem_do_endereco NOT LIKE '%[a-z]%') OR
                 origem_do_endereco LIKE '%@%' THEN origem_do_endereco
           END origem
         , CASE
             WHEN LOWER(cpf_cnpj) LIKE '%[a-z]%' OR cpf_cnpj IS NULL THEN
                 CASE
                     WHEN alloha_cpf_cnpj LIKE '%[a-z]%' THEN NULL
                     WHEN LEN(alloha_cpf_cnpj) < 9 OR LEN(alloha_cpf_cnpj) > 14 THEN NULL
                     WHEN LEN(alloha_cpf_cnpj) > 11 THEN REPLICATE('0', 14 - LEN(alloha_cpf_cnpj)) + RTRIM(alloha_cpf_cnpj)
                     ELSE REPLICATE('0', 11 - LEN(alloha_cpf_cnpj)) + RTRIM(alloha_cpf_cnpj)
                 END
             ELSE
                 CASE
                     WHEN LEN(cpf_cnpj) < 9 OR LEN(cpf_cnpj) > 14 THEN NULL
                     WHEN LEN(cpf_cnpj) > 11 THEN REPLICATE('0', 14 - LEN(cpf_cnpj)) + RTRIM(cpf_cnpj)
                     ELSE REPLICATE('0', 11 - LEN(cpf_cnpj)) + RTRIM(cpf_cnpj)
                 END
           END cpf_cnpj
         , IIF(contrato IS NULL, alloha_contrato, contrato) AS contrato
         , cidade, cep, bairro, pergunta_satisfacao_1, pergunta_satisfacao_2, pergunta_satisfacao_3
         , tempo_interacao, tempo_conversa, tempo_espera_fila, apos_horario_trabalho, transferida_de
         , transferida_para, transferencias, data_extracao
         , IIF(marca_padrao IS NOT NULL, marca_padrao, IIF(marca_alloha IS NOT NULL, marca_alloha, marca_outros)) AS marca

    FROM cte_tratamentos_preliminares
)
INSERT INTO STAGE.five9.stg_chamadas_chat_v2
SELECT
    id_chamada, data_inicio_chamada, campanha, agente, tipo_midia, subtipo_midia, resultado, motivo_retencao_ura
     , cliente, cpf_cnpj, contrato, origem
     , CASE
         WHEN origem IS NULL THEN NULL
         WHEN origem LIKE '%@%' THEN 'Chat'
         ELSE 'Telefone'
      END flg_chat_ou_telefone
     , cidade, cep, bairro, pergunta_satisfacao_1, pergunta_satisfacao_2, pergunta_satisfacao_3
     , tempo_interacao, tempo_conversa, tempo_espera_fila, apos_horario_trabalho, transferida_de, transferida_para
     , transferencias, marca
     , CASE
         WHEN marca IN ('SUMICITY', 'GIGA+ FIBRA', 'CLICK', 'UNIVOX') THEN 'SUMICITY'
         WHEN marca IN ('VIP', 'LIGUE', 'NIU') THEN 'VIP'
         WHEN marca = 'MOBWIRE' THEN 'MOB'
       END AS polo
     , data_extracao

FROM cte_tratamento_final

END
```
