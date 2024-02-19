DECLARE @INI DATETIME, @FIM DATETIME, @MSG NVARCHAR(MAX);
DECLARE @DATA_ATUAL DATE = '2024-02-17'

DECLARE @ATUALIZACAO_CONTATO_TELEFONE DATETIME = (SELECT MAX(CAST([data_extracao] AS DATE)) atualizacao_contato_telefone FROM [DBDW].[dbo].[dim_contato_telefone])
DECLARE @ATUALIZACAO_CONTATO_EMAIL DATE = (SELECT MAX(CAST([data_extracao] AS DATE)) atualizacao_contato_email FROM [DBDW].[dbo].[dim_contato_email])

IF @ATUALIZACAO_CONTATO_TELEFONE < CAST(@DATA_ATUAL AS DATE) OR @ATUALIZACAO_CONTATO_EMAIL < CAST(@DATA_ATUAL AS DATE)
BEGIN	
	IF @ATUALIZACAO_CONTATO_TELEFONE <> CAST(GETDATE() AS DATE)
	BEGIN
		SET @INI = DATEADD(HOUR, -3, getdate());
		SET @FIM = DATEADD(HOUR, -3, getdate());
		SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
		EXEC DBDW.mailing.InsertLog
			@PROCESSO = @MSG,
			@DataHora_I = @INI, 
			@DataHora_F =	@FIM,
			@MENSAGEM = 'Tabela de contato telefone não atualizada';
	END
	ELSE
	BEGIN
		SET @INI = DATEADD(HOUR, -3, getdate());
		SET @FIM = DATEADD(HOUR, -3, getdate());
		SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
		EXEC DBDW.mailing.InsertLog
			@PROCESSO = @MSG,
			@DataHora_I = @INI, 
			@DataHora_F =	@FIM,
			@MENSAGEM = 'Tabela de contato email não atualizada';
	END
END
ELSE
BEGIN

	SET @INI = DATEADD(HOUR, -3, getdate());

	DROP TABLE IF EXISTS 
	#tmp_SMS,
	DBDW.mailing.contatos,
	#tmp_whatsapp,
	#tmp_email;

	WITH cte_email AS (
	SELECT
		id_cliente,    
		contato EMAIL,
		ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) prioridade
	  FROM [DBDW].[dbo].[vw_dim_contato_email] tcontato
	  WHERE 
		tcontato.tipo = 'EMAIL' 
		AND tcontato.excluido = 0 AND
		NOT(
			tcontato.contato NOT LIKE '%@%' 
			OR tcontato.contato LIKE '%atualiz%' 
			OR tcontato.contato LIKE '%sac@sumicity%' 
			OR tcontato.contato LIKE '%autalize%' 
			or tcontato.contato LIKE '%teste%' 
			OR tcontato.contato LIKE '%naotem%' 
			OR tcontato.contato LIKE '%n_oconsta%' 
			OR tcontato.contato LIKE '%rca@%' 
			OR tcontato.contato LIKE '%sumi%' 
			OR tcontato.contato LIKE '%@tem%')
	)
	SELECT 
		id_cliente,    
		EMAIL
	INTO #tmp_email
	FROM cte_email
	WHERE prioridade = 1;


	SELECT
		id_cliente,
		CONCAT('+55', telefone_tratado) 'WHATSAPP'
	INTO #tmp_whatsapp
	FROM DBDW.dbo.vw_dim_contato_telefone
	WHERE  fonte_contato LIKE 'sumicity%';

	WITH cte_contatos AS (
	SELECT
		id_cliente,
		CONCAT('+55', telefone_tratado) telefone,
		ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) r_num
	FROM DBDW.dbo.vw_dim_contato_telefone
	WHERE 
		excluido = 0
		AND classificacao_contato in ('MOVEL')
		AND existe_air = 'S'
		AND NOT fonte_contato LIKE 'sumicity%'
	)
	SELECT 
		id_cliente,
		telefone
	INTO #tmp_SMS
	FROM cte_contatos
	WHERE r_num = 1;

	WITH cte_contatos AS (
	SELECT
		id_cliente,
		CONCAT('+55', telefone_tratado) telefone,
		ROW_NUMBER() OVER(PARTITION BY id_cliente ORDER BY data_criacao DESC) prioridade
	FROM DBDW.dbo.vw_dim_contato_telefone
	WHERE 
		excluido = 0
		AND classificacao_contato in ('MOVEL','FIXO')
		AND existe_air = 'S'
		AND NOT fonte_contato LIKE 'sumicity%'
	)
	SELECT 
		dct.id_cliente,
		MAX(TEL_01) AS tel_01,
		MAX(TEL_02) AS tel_02,
		MAX(TEL_03) AS tel_03,
		MAX(TEL_04) AS tel_04,
		MAX(TEL_05) AS tel_05,
		MAX(TEL_06) AS tel_06,
		MAX(WHATSAPP) AS whatsapp,
		MAX(s.telefone) AS sms,
		MAX(EMAIL) AS email,
		DATEADD(HOUR, -3, getdate()) AS data_extracao
	INTO DBDW.mailing.contatos
	FROM DBDW.dbo.vw_dim_contato_telefone dct
	LEFT JOIN (
		SELECT 
			id_cliente,
			telefone,
			CASE
				WHEN prioridade = 1
				THEN telefone
				ELSE NULL
			END TEL_01,
			CASE
				WHEN prioridade = 2
				THEN telefone
				ELSE NULL
			END TEL_02,
			CASE
				WHEN prioridade = 3
				THEN telefone
				ELSE NULL
			END TEL_03,
			CASE
				WHEN prioridade = 4
				THEN telefone
				ELSE NULL
			END TEL_04,
			CASE
				WHEN prioridade = 5
				THEN telefone
				ELSE NULL
			END TEL_05,
			CASE
				WHEN prioridade = 6
				THEN telefone
				ELSE NULL
			END TEL_06
		FROM cte_contatos
	) AS PivotData
		ON dct.id_cliente = PivotData.id_cliente
	LEFT JOIN #tmp_whatsapp ws
		ON dct.id_cliente = ws.id_cliente
	LEFT JOIN #tmp_email e
		ON dct.id_cliente = e.id_cliente
	LEFT JOIN #tmp_SMS s
		ON  dct.id_cliente = s.id_cliente
	GROUP BY dct.id_cliente;

	SET @FIM = DATEADD(HOUR, -3, getdate());

	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
		EXEC DBDW.mailing.InsertLog
			@PROCESSO = @MSG,
			@DataHora_I = @INI, 
			@DataHora_F = @FIM,
			@MENSAGEM = 'Atualizando contatos telefônicos';

END;

