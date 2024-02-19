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


--### MOTIVOS FATURAS ###--
	--Baixa operacional
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET 
			mi.data_saida = @DATA_ATUAL,
			mi.data_motivo_saida = CONVERT(DATE, fm.data_negociacao),
			mi.data_atualizacao_motivo_saida = CONVERT(DATE, fm.data_atualizacao),
			mi.motivo_saida =  'BAIXA_OPERACIONAL',
			mi.saida_tipo = 'FATURA'
		FROM DBDW.mailing.analitico mi
		LEFT JOIN DBDW.sydle.dim_faturas_mailing fm
			ON mi.id_contrato = fm.contrato_air AND mi.fatura_motivadora = fm.codigo_fatura_sydle AND mi.versao_fatura < fm.versao
		WHERE  mi.data_saida IS NULL
			AND fm.status_fatura = 'Baixa Operacional'
			AND fm.data_atualizacao < @DATA_ATUAL
			AND mi.data_referencia = @DATA_MAILING;
	
	SET @FIM = DATEADD(HOUR, -3, getdate());
	
	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando faturas foram com baixa operacional';

	-- Pagamento espontaneo
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET 
			mi.data_saida = 
				CASE 
					WHEN mi.data_saida IS NULL
						THEN @DATA_ATUAL
					ELSE mi.data_saida END,
			mi.data_motivo_saida = 
				CASE 
					WHEN (mi.data_saida IS NULL OR mi.motivo_saida = 'BAIXA_OPERACIONAL') 
						THEN CONVERT(DATE, fm.data_pagamento)
					ELSE mi.data_motivo_saida END,
			mi.data_atualizacao_motivo_saida = 
				CASE 
					WHEN (mi.data_saida IS NULL OR mi.motivo_saida = 'BAIXA_OPERACIONAL') 
						THEN CONVERT(DATE, fm.data_atualizacao)
					ELSE mi.data_atualizacao_motivo_saida END,
			mi.motivo_saida = 
				CASE 
					WHEN (mi.data_saida IS NULL OR mi.motivo_saida = 'BAIXA_OPERACIONAL') 
						THEN 'PAGAMENTO'
					ELSE mi.motivo_saida END,
			mi.saida_tipo =
				CASE 
					WHEN (mi.data_saida IS NULL OR mi.motivo_saida = 'BAIXA_OPERACIONAL') 
						THEN 'FATURA'
					ELSE mi.saida_tipo END,
			mi.data_pagamento = CONVERT(DATE, fm.data_pagamento),
			mi.valor_pago = fm.valor_pago,
			mi.tipo_pagamento = 'ESPONTÂNEO'
		FROM DBDW.mailing.analitico mi
		LEFT JOIN DBDW.sydle.dim_faturas_mailing fm 
			ON mi.id_contrato = fm.contrato_air AND mi.fatura_motivadora = fm.codigo_fatura_sydle AND mi.versao_fatura < fm.versao
		WHERE  
			fm.status_fatura = 'Paga'
			AND fm.data_atualizacao < @DATA_ATUAL
			AND mi.data_referencia = @DATA_MAILING;
	
	SET @FIM = DATEADD(HOUR, -3, getdate());
	
	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando faturas foram pagas espontaneamente';

	-- Negociacao
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET 
			mi.data_saida = @DATA_ATUAL,
			mi.data_motivo_saida = COALESCE(CONVERT(DATE, fm.data_negociacao), CONVERT(DATE, fm.data_atualizacao)),
			mi.data_atualizacao_motivo_saida = CONVERT(DATE, fm.data_atualizacao),
			mi.motivo_saida =  'NEGOCIAÇÃO',
			mi.saida_tipo = 'FATURA'
		FROM DBDW.mailing.analitico mi
		LEFT JOIN DBDW.sydle.dim_faturas_mailing fm
			ON mi.id_contrato = fm.contrato_air AND mi.fatura_motivadora = fm.codigo_fatura_sydle AND mi.versao_fatura < fm.versao
		WHERE  mi.data_saida IS NULL
			AND fm.status_fatura = 'Negociada com o cliente'
			AND fm.data_atualizacao < @DATA_ATUAL
			AND mi.data_referencia = @DATA_MAILING;
	
	SET @FIM = DATEADD(HOUR, -3, getdate());
	
	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando faturas foram negociadas';

	--Abono
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET 
			mi.data_saida = @DATA_ATUAL,
			mi.data_motivo_saida = CONVERT(DATE, fm.data_negociacao),
			mi.data_atualizacao_motivo_saida = CONVERT(DATE, fm.data_atualizacao),
			mi.motivo_saida =  'ABONO',
			mi.saida_tipo = 'FATURA'
		FROM DBDW.mailing.analitico mi
		LEFT JOIN DBDW.sydle.dim_faturas_mailing fm
			ON mi.id_contrato = fm.contrato_air AND mi.fatura_motivadora = fm.codigo_fatura_sydle AND mi.versao_fatura < fm.versao
		WHERE  mi.data_saida IS NULL
			AND fm.status_fatura = 'Abonada'
			AND fm.data_atualizacao < @DATA_ATUAL
			AND mi.data_referencia = @DATA_MAILING;

	SET @FIM = DATEADD(HOUR, -3, getdate());
	
	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando faturas foram abonadas';

	--Cancelamento
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET 
			mi.data_saida = @DATA_ATUAL,
			mi.data_motivo_saida = CONVERT(DATE, fm.data_negociacao),
			mi.data_atualizacao_motivo_saida = CONVERT(DATE, fm.data_atualizacao),
			mi.motivo_saida =  'CANCELAMENTO',
			mi.saida_tipo = 'FATURA'
		FROM DBDW.mailing.analitico mi
		LEFT JOIN DBDW.sydle.dim_faturas_mailing fm
			ON mi.id_contrato = fm.contrato_air AND mi.fatura_motivadora = fm.codigo_fatura_sydle AND mi.versao_fatura < fm.versao
		WHERE  mi.data_saida IS NULL
			AND fm.status_fatura = 'Cancelada'
			AND fm.data_atualizacao < @DATA_ATUAL
			AND mi.data_referencia = @DATA_MAILING;
	
	SET @FIM = DATEADD(HOUR, -3, getdate());
	
	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando faturas foram canceladas'
END;