DECLARE @DATA_MAILING DATE, @INI DATETIME, @FIM DATETIME, @MSG NVARCHAR(MAX);

--### Validações ###--
DECLARE @DATA_ATUAL DATE = '2024-02-17'
DECLARE @TABELAS_DESATUALIZADAS NVARCHAR(200) =   
	(SELECT DISTINCT MensagemLog FROM [DBDW].[mailing].[log] 
	WHERE ProcessoExecucao = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	AND (MensagemLog = 'Tabela de contratos não atualizada' OR MensagemLog = 'Tabela de faturas não atualizada'))


SET @TABELAS_DESATUALIZADAS = NULL
IF @TABELAS_DESATUALIZADAS IS NOT NULL
BEGIN
	SELECT 'TABELAS DESATUALIZADAS'
END
ELSE
BEGIN
--### Inicio limpeza mailing ###--

	IF DAY(@DATA_ATUAL) = 1
	BEGIN
	   SET @DATA_MAILING = DATEADD(MONTH, -1, DATEADD(DAY, -DAY(@DATA_ATUAL)+1, @DATA_ATUAL))
	END
	ELSE
	BEGIN
		SET @DATA_MAILING = DATEADD(DAY, -DAY(@DATA_ATUAL)+1, @DATA_ATUAL)
	END


--### Atualização aging atual ###--
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET mi.aging_atual = DATEDIFF(DAY, mi.data_vencimento_antigo, @DATA_ATUAL)
		FROM DBDW.mailing.analitico mi
		WHERE mi.data_referencia = @DATA_MAILING
	
	SET @FIM = DATEADD(HOUR, -3, getdate());

	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Atualização do aging atual'
END;