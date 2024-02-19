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


--### MOTIVOS CONTRATOS ###--
	-- CANCELAMENTO: VOLUNTÁRIO / INVOLUNTÁRIO / INVOLUNTÁRIO ADICIONAL (Débito / FPD / ERRO)
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET
			mi.data_saida = @DATA_ATUAL,
			mi.data_motivo_saida = ca.data_cancelamento,
			mi.data_atualizacao_motivo_saida = co.data_alteracao,
			mi.saida_tipo = 'CONTRATO',
			mi.motivo_saida = 'CANCELAMENTO',
			mi.data_cancelamento_contrato = ca.data_cancelamento,
			mi.motivo_cancelamento_contrato = 
				CASE 
					WHEN ca.cancelamento_invol IS NULL THEN 'VOLUNTÁRIO'
					WHEN ca.cancelamento_invol = 'SISTEMICO' THEN 'INVOLUNTÁRIO'
					ELSE 'INVOLUNTÁRIO_ADICIONAL'
				END
		FROM DBDW.mailing.analitico mi
		LEFT JOIN  DBDW.dbo.fato_cancelamento ca
			ON mi.id_contrato = ca.id_contrato
		INNER JOIN DBDW.dbo.dim_contrato_v2 co
			ON ca.id_contrato = co.id_contrato AND ca.data_cancelamento = co.data_cancelamento AND mi.versao_contrato < co.versao
		WHERE 
			mi.data_saida IS NULL
			AND mi.status_mailing = 'ATIVO'
			AND co.data_alteracao < @DATA_ATUAL
			AND mi.data_referencia = @DATA_MAILING;

	SET @FIM = DATEADD(HOUR, -3, getdate());

	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando contratos cancelados';

	-- TROCA TITULARIDADE
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET 
			mi.data_saida = @DATA_ATUAL,
			mi.data_atualizacao_motivo_saida = CONVERT(DATE, co.data_alteracao),
			mi.motivo_saida =  'TROCA_TITULARIDADE',
			mi.saida_tipo = 'CONTRATO'
		FROM DBDW.mailing.analitico mi
		LEFT JOIN DBDW.dbo.dim_contrato_v2 co 
			ON mi.id_contrato = co.id_contrato
		WHERE 
			mi.data_saida IS NULL
			AND mi.id_cliente <> co.id_cliente
			AND mi.versao_contrato + 1 = co.versao
			AND co.data_alteracao < @DATA_ATUAL
			AND mi.data_referencia = @DATA_MAILING;;
	
	SET @FIM = DATEADD(HOUR, -3, getdate());
	
	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando contratos que trocaram titularidade';

	-- TROCA SEGMENTO
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET 
			mi.data_saida = @DATA_ATUAL,
			mi.data_atualizacao_motivo_saida = CONVERT(DATE, co.data_alteracao),
			mi.motivo_saida =  'TROCA_SEGMENTO',
			mi.saida_tipo = 'CONTRATO'
		FROM DBDW.mailing.analitico mi
		INNER JOIN DBDW.dbo.dim_contrato_v2 co
			ON mi.id_contrato = co.id_contrato AND mi.versao_Contrato + 1 = co.versao AND co.b2b = 1
		WHERE 
			mi.data_saida IS NULL
			AND co.data_alteracao < @DATA_ATUAL
			AND mi.data_referencia = @DATA_MAILING;;
	
	SET @FIM = DATEADD(HOUR, -3, getdate());
	
	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando contratos que trocaram segmento'

END;