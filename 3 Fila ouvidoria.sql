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


--### FILA OUVIDORIA e CONTESTAÇÃO FATURA ###--
	SET @INI = DATEADD(HOUR, -3, getdate());

		DROP TABLE IF EXISTS 
		#tmp_contestacao_ouvidoria;

		SELECT 
			codigo_contrato
		INTO #tmp_contestacao_ouvidoria
		FROM DBDW.mailing.analitico mi
		LEFT JOIN DBDW.chamados.vw_dim_chamado cha
			ON mi.id_contrato = cha.codigo_contrato AND mi.status_mailing = 'ATIVO' AND mi.data_saida IS NULL AND mi.data_referencia = @DATA_MAILING
		WHERE data_conclusao is null and (nome_fila = 'FINANCEIRO - CONTESTAÇÃO DE FATURA' OR nome_fila LIKE 'Ouvidoria%');
		
		UPDATE mi
		SET
			mi.fila_ouvidoria_contestacao_fatura = 
				CASE 
					WHEN co.codigo_contrato IS NOT NULL 
						THEN 1
					ELSE 0 
				END
		FROM DBDW.mailing.analitico mi
		LEFT JOIN  #tmp_contestacao_ouvidoria co
			ON mi.id_contrato = co.codigo_contrato
		WHERE 
			mi.data_saida IS NULL
			AND mi.status_mailing = 'ATIVO'
			AND mi.data_referencia = @DATA_MAILING;

	SET @FIM = DATEADD(HOUR, -3, getdate());

	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'atualizando dados ouvidoria e contestação de fatura'
END;