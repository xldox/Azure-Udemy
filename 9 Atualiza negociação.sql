DECLARE @INI DATETIME, @FIM DATETIME, @MSG NVARCHAR(MAX),@DATA_MAILING DATE;
DECLARE @DATA_ATUAL DATE = '2024-02-17'

IF DAY(@DATA_ATUAL) = 1
	BEGIN
	   SET @DATA_MAILING = DATEADD(MONTH, -1, DATEADD(DAY, -DAY(@DATA_ATUAL)+1, @DATA_ATUAL))
	END
	ELSE
	BEGIN
		SET @DATA_MAILING = DATEADD(DAY, -DAY(@DATA_ATUAL)+1, @DATA_ATUAL)
	END


SET @INI = GETDATE();
 
		UPDATE mi
		SET mi.negociacao = COALESCE(tbl_ultpag.negociacao,0)
		FROM DBDW.mailing.analitico mi
		LEFT JOIN (
			SELECT contrato_air, MAX(CASE WHEN classificacao = 'Negociação' THEN 1 ELSE 0 END) negociacao
			FROM DBDW.sydle.vw_faturas_mailing vw
			LEFT JOIN  DBDW.mailing.analitico mi
				ON vw.contrato_air = mi.id_contrato AND  mi.data_referencia = @DATA_MAILING
			WHERE vw.status_fatura IN ('Emitida','Aguardando pagamento em sistema externo','Aguardando baixa em sistema externo')
			GROUP BY contrato_air
		) tbl_ultpag
			ON mi.id_contrato = tbl_ultpag.contrato_air
		WHERE mi.data_referencia = @DATA_MAILING
	SET @FIM = GETDATE();
 
	SET @MSG = CONCAT('ATUALIZAÇÃO MAILING - ', CONVERT(nvarchar, @DATA_ATUAL))
	EXEC DBDW.mailing.InsertLog
		@PROCESSO = @MSG,
		@DataHora_I = @INI, 
		@DataHora_F =	@FIM,
		@MENSAGEM = 'Atualização da negociação';