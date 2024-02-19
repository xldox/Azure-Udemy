DECLARE @DATA_MAILING DATE, @INI DATETIME, @FIM DATETIME, @MSG NVARCHAR(MAX);

--### Valida��es ###--
DECLARE @DATA_ATUAL DATE = '2024-02-17'
DECLARE @TABELAS_DESATUALIZADAS NVARCHAR(200) =   
	(SELECT DISTINCT MensagemLog FROM [DBDW].[mailing].[log] 
	WHERE ProcessoExecucao = CONCAT('ATUALIZA��O MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	AND (MensagemLog = 'Tabela de contratos n�o atualizada' OR MensagemLog = 'Tabela de faturas n�o atualizada'))

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


--### Pagamentos Negocia��es ###--
	SET @INI = DATEADD(HOUR, -3, getdate());

		UPDATE mi
		SET
			mi.data_pagamento =  CAST(fm.data_pagamento AS DATE),
			mi.tipo_pagamento = 'NEGOCIA��O',
			mi.valor_pago = fm.valor_pago
		FROM DBDW.mailing.analitico mi
		LEFT JOIN (
			SELECT DISTINCT
					m.fatura_motivadora,
					neg.codigo
			FROM DBDW.mailing.analitico m
			LEFT JOIN DBDW.sydle.vw_negociacoes neg
				ON m.fatura_motivadora = neg.fatura_original
			WHERE m.motivo_saida = 'NEGOCIA��O'
			AND m.data_referencia = @DATA_MAILING
		) as a
			ON mi.fatura_motivadora = a.fatura_motivadora
		LEFT JOIN DBDW.sydle.vw_faturas_mailing fm
			ON fm.codigo_fatura_sydle = a.codigo
		WHERE 
			a.fatura_motivadora IS NOT NULL
			AND fm.data_pagamento IS NOT NULL
			AND mi.data_referencia = @DATA_MAILING
	
	SET @FIM = DATEADD(HOUR, -3, getdate());
	
	SET @MSG = CONCAT('ATUALIZA��O MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Verificando pagamento das faturas j� negociadas'

END;