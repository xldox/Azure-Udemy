# Tabela Fato de Chamadas Recebidas Voz

Esta tabela armazena as informações das chamadas recebidas para o tipo de mídia voz do sistema Five9.

## Colunas

| Nome da Coluna          | Tipo                 | Descrição                                                                                       |
|-------------------------|----------------------|-------------------------------------------------------------------------------------------------|
| id                      | int (auto increment) | Identificador único da tabela                                                                   |
| data_input              | varchar(max)         | Data de inserção dos registros.                                                                 |
| data_extracao           | varchar(max)         | Data de extração dos registros via API.                                                         |
| id_chamada              | varchar(max)         | Identificador único da chamada.                                                                 |
| data_inicio_chamada     | datetime             | Data de início da chamada.                                                                      |
| data_inicio_atendimento | datetime             | Data de início de atendimento com um agente.                                                    |
| campanha                | varchar(max)         | Nome da campanha cadastrada no Five9 em que a ligação está atrelada.                            |
| agente                  | varchar(max)         | Nome do agente que atendeu a ligação.                                                           |
| servico                 | varchar(max)         | Nome do serviço para qual a ligação navegou.                                                    |
| tipo_segmento           | varchar(max)         | Tipo do segmento no qual atribuiu para a ligação.                                               |
| destino_transferencia   | varchar(max)         | Serviço para o qual a ligação foi transferida.                                                  |
| resultado               | varchar(max)         | Resultado final da ligação.                                                                     |
| servico_final           | varchar(max)         | Último serviço no qual a ligação foi transferida.                                               |
| classificao_grupo_a     | varchar(max)         |                                                                                                 |
| classificao_grupo_b     | varchar(max)         |                                                                                                 |
| classificao_grupo_c     | varchar(max)         |                                                                                                 |
| motivo_retencao_ura     | varchar(max)         | O motivo no qual fez a ligação ser retida na URA ao invés de ser transferida para um atendente. |
| tipo_chamada            | varchar(max)         | Classificação da chamada quanto a recebida ou ativa.                                            |
| cpf_cnpj                | varchar(max)         | Número do documento do cliente.                                                                 |
| contrato                | varchar(max)         | Número do contrato do cliente.                                                                  |
| origem                  | varchar(max)         | A origem da ligação. Número do telefone do cliente.                                             |
| destino                 | varchar(max)         | O destino da ligação. Número interno da empresa na qual a ligação foi direcionada.              |
| transferida             | varchar(max)         | Flag que representa se a ligação foi transferida pelo agente.                                   |
| abandonada              | varchar(max)         | Flag que represente se a ligação foi abandonada pelo cliente.                                   |
| massiva                 | varchar(max)         | Flag se houveu massiva.                                                                         |
| cidade                  | varchar(max)         | Nome da cidade do cliente.                                                                      |
| bairro                  | varchar(max)         | Nome do bairro do cliente.                                                                      |
| endereco                | varchar(max)         | Endereço completo do cliente.                                                                   |
| pergunta_satisfacao_1   | varchar(max)         |                                                                                                 |
| pergunta_satisfacao_2   | varchar(max)         |                                                                                                 |
| pergunta_satisfacao_3   | varchar(max)         |                                                                                                 |
| tempo_chamada           | varchar(max)         | Tempo total que durou a ligação.                                                                |
| tempo_conversa          | varchar(max)         | Tempo total de conversa entre o cliente e o agente.                                             |
| tempo_espera_fila       | varchar(max)         | Tempo total em que o cliente teve de esperar na fila.                                           |
| tempo_toque             | varchar(max)         |                                                                                                 |
| tempo_pos_atendimento   | varchar(max)         | Tempo total no qual o cliente ficou aguardando após atendimento.                                |
| desligado_por           | varchar(max)         | Flag que mostra quem desligou primeiro: cliente ou agente.                                      |
| marca                   | varchar(max)         | Nome da marca para qual a ligação foi atribuída.                                                |
| polo                    | varchar(max)         | Nome do polo para qual a ligação foi atribuído.                                                 |
