DECLARE @DATA_MAILING DATE, @INI DATETIME, @FIM DATETIME, @MSG NVARCHAR(MAX);

--### Validações ###--
DECLARE @DATA_ATUAL DATE = '2024-02-17'
DECLARE @ATUALIZACAO_FATURAS DATETIME = (SELECT MAX(data_atualizacao) atualizacao_faturas FROM [DBDW].[sydle].[dim_faturas_mailing])
DECLARE @ATUALIZACAO_CONTRATOS DATE = (SELECT MAX(CAST(data_extracao AS DATE)) FROM [DBDW].[dbo].[dim_contrato_v2])

IF @ATUALIZACAO_CONTRATOS < CAST(@DATA_ATUAL AS DATE) OR @ATUALIZACAO_FATURAS < CONCAT(DATEADD(DAY, -1, CAST(@DATA_ATUAL AS DATE)),'T20:00:00')
BEGIN	
	IF @ATUALIZACAO_CONTRATOS <> CAST(GETDATE() AS DATE)
	BEGIN
		SET @INI = DATEADD(HOUR, -3, getdate());
		SET @FIM = DATEADD(HOUR, -3, getdate());
		SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
		EXEC DBDW.mailing.InsertLog
			@PROCESSO = @MSG,
			@DataHora_I = @INI, 
			@DataHora_F =	@FIM,
			@MENSAGEM = 'Tabela de contratos não atualizada';
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
			@MENSAGEM = 'Tabela de faturas não atualizada';
	END
END
ELSE
BEGIN
	SELECT 'TABELAS ATUALIZADAS'
END;