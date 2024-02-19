"""Módulo de manipulação de solicitações HTTP."""

from enum import Enum
from typing import Dict

import httpx
from loguru import logger
from pydantic import BaseModel


class HTTPMETHODS(Enum):
    """Esta classe é uma enumeração de métodos HTTP."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"

    def upper(self) -> str:
        """Retorna o nome do método HTTP em letras maiúsculas."""
        return self.value


class HTTPRequest(BaseModel):
    """
    Classe que representa uma solicitação HTTP genérica.

    Args:
        url (str): A URL da solicitação.
        method (str): O método HTTP a ser usado (padrão é "GET").
        headers (dict): Um dicionário de cabeçalhos HTTP personalizado.
        params (dict): Um dicionário de parâmetros de consulta.
        data (str): Uma string de dados a serem enviados na solicitação.
    """

    url: str
    method: HTTPMETHODS = "GET"
    headers: Dict = None
    params: Dict = None
    data: str


@logger.catch(level="CRITICAL", message="Erro causado na função make_http_request().")
async def make_http_request(http_request: HTTPRequest) -> str:
    """
    Faz uma solicitação HTTP genérica.

    Args:
        http_request (HTTPRequest): Um objeto HTTPRequest contendo os detalhes da solicitação.

    Returns:
        str: A resposta da solicitação HTTP.
    """
    async with httpx.AsyncClient() as client:
        response = await client.request(
            http_request.method,
            http_request.url,
            headers=http_request.headers,
            params=http_request.params,
            data=http_request.data,
        )
        logger.info(response.raise_for_status())
        return response.text
