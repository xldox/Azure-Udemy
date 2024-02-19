DECLARE @DATA_ATUAL DATE, @DATA_ATUAL_EXTERNA DATE

SET @DATA_ATUAL = CAST(GETDATE() AS DATE)
SET @DATA_ATUAL_EXTERNA = NULL -- Inicialize a vari�vel externa conforme necess�rio

IF @DATA_ATUAL_EXTERNA IS NULL
	BEGIN 
		INSERT INTO TESTE.dbo.teste_variavel
		SELECT @DATA_ATUAL
	END
ELSE 
	BEGIN
		INSERT INTO TESTE.dbo.teste_variavel
		SELECT @DATA_ATUAL_EXTERNA
	END

----select * from TESTE.dbo.teste_variavel
--TRUNCATE TABLE TESTE.dbo.teste_variavel
