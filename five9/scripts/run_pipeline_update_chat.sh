#!/bin/bash

exec >> /var/bi-etl/five9/logs/log_run_sh_pipeline_update_chat.txt 2>&1

# Verifique o UID para determinar se o script está sendo executado como root
if [ "$UID" -eq 0 ]; then
    echo "Este script está sendo executado como root."
else
    echo "Este script não está sendo executado como root."
fi

echo "Início do script em $(date)"

cd /var/bi-etl/five9
echo "Diretório atual: $(pwd)"

echo "Rodando o ETL: /root/.local/bin/poetry run python3 -m src.pipeline_update_chat"
/root/.local/bin/poetry run python3 -m src.pipeline_update_chat

echo "Script concluído em $(date)"
