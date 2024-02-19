"""Módulo responsável pela extração dos dados da plataforma da Five9."""

import asyncio
import base64
import os

import xmltodict
from dotenv import load_dotenv
from loguru import logger

from .handler_http import HTTPMETHODS, HTTPRequest, make_http_request

# Carregar variáveis de ambiente do arquivo .env que está na raíz do projeto
env_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env"
)

load_dotenv(dotenv_path=env_path)

FIVE9_CLIENT_ID = os.environ.get("FIVE9_CLIENT_ID")
FIVE9_CLIENT_SECRET = os.environ.get("FIVE9_CLIENT_SECRET")

AUTH_URL = "https://api.five9.com/wsadmin/v13/AdminWebService"
auth_header = base64.b64encode(
    f"{FIVE9_CLIENT_ID}:{FIVE9_CLIENT_SECRET}".encode("utf-8")
).decode("utf-8")
HEADERS = {
    "Authorization": f"Basic {auth_header}",
    "Content-Type": "application/xml;charset=utf-8",
    "Accept-Encoding": "gzip, deflate, br",
    "SOAPAction": "",
}


@logger.catch(
    level="CRITICAL", message="Erro causado na função get_five9_identifier_report()."
)
async def get_five9_identifier_report(
    folder_name: str, report_name: str, startAt: str, endAt: str
) -> str:
    """
    Obtém o identificador (ID) dos reports da Five9.

    Args:
        folder_name (str): Nome do diretório padrão no qual os reports ficam salvos na Five9.
        report_name (str): Nome do report a ser obtido do diretório padrão.
        startAt (str): Data inicial desejada de extração.
        endAt (str): Data final desejada de extração.

    Returns:
        str: Identificador único (ID) do report.
    """
    data = f"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
            <soapenv:Header/>
            <soapenv:Body>
                <ser:runReport>
                    <folderName>{folder_name}</folderName>
                    <reportName>{report_name}</reportName>
                    <criteria>
                        <time>
                            <end>{endAt}</end>
                            <start>{startAt}</start>
                        </time>
                    </criteria>
                </ser:runReport>
            </soapenv:Body>
        </soapenv:Envelope>"""

    response = await make_http_request(
        HTTPRequest(url=AUTH_URL, method=HTTPMETHODS.POST, headers=HEADERS, data=data)
    )

    return xmltodict.parse(response)["env:Envelope"]["env:Body"][
        "ns2:runReportResponse"
    ]["return"]


@logger.catch(
    level="CRITICAL", message="Erro causado na função checks_report_is_running()."
)
async def checks_report_is_running(identifier: str) -> str:
    """
    Verifica se o report já está pronto para uso e retorna o status correspondente.

    Args:
        identifier (str): ID do report gerado.

    Returns:
        str: Expressão 'true' ou 'false' indicando se o report ainda está sendo gerado.
    """
    data = f"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
            <soapenv:Header/>
            <soapenv:Body>
                <ser:isReportRunning>
                    <identifier>{identifier}</identifier>
                </ser:isReportRunning>
            </soapenv:Body>
        </soapenv:Envelope>
    """

    response = await make_http_request(
        HTTPRequest(url=AUTH_URL, method=HTTPMETHODS.POST, headers=HEADERS, data=data)
    )

    return xmltodict.parse(response)["env:Envelope"]["env:Body"][
        "ns2:isReportRunningResponse"
    ]["return"]


@logger.catch(
    level="CRITICAL", message="Erro causado na função get_report_result_as_csv()."
)
async def get_report_result_as_csv(
    folder_name: str, report_name: str, startAt: str, endAt: str
) -> str:
    """
    Obtém como resultado o report do Five9.

    Args:
        folder_name (str): Nome do diretório padrão no qual os reports ficam salvos na Five9.
        report_name (str): Nome do report a ser obtido do diretório padrão.
        startAt (str): Data inicial desejada de extração.
        endAt (str): Data final desejada de extração.

    Returns:
        str: Report no formato CSV.
    """
    identifier = await get_five9_identifier_report(
        folder_name, report_name, startAt, endAt
    )

    while True:
        if await checks_report_is_running(identifier) == "true":
            await asyncio.sleep(15)
            continue
        else:
            break
    await asyncio.sleep(15)

    data = f"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
            <soapenv:Header/>
            <soapenv:Body>
                <ser:getReportResultCsv>
                    <identifier>{identifier}</identifier>
                </ser:getReportResultCsv>
            </soapenv:Body>
        </soapenv:Envelope>
    """

    response = await make_http_request(
        HTTPRequest(url=AUTH_URL, method=HTTPMETHODS.POST, headers=HEADERS, data=data)
    )

    return xmltodict.parse(response)["env:Envelope"]["env:Body"][
        "ns2:getReportResultCsvResponse"
    ]["return"]


if __name__ == "__main__":
    import nest_asyncio

    nest_asyncio.apply()

    folder_name = "Meus relatorios"
    report_name = "Report Chat - Interacoes Digitais"
    startAt = "2023-09-25T00:00:00.000-03:00"
    endAt = "2023-09-25T23:59:59.000-03:00"

    asyncio.run(get_report_result_as_csv(folder_name, report_name, startAt, endAt))
