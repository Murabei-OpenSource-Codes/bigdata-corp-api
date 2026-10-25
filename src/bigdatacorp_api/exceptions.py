"""Custom exceptions for BigDataCorp API client errors."""


def _extract_status_codes(bigdata_response: dict) -> list[dict]:
    """Extract the status codes from the BigDataCorp response.

    The response from bigdata status is on a dictionary with keys that
    vary from one request to another. This function extracts the status
    codes from the response and returns a list of dictionaries with the
    key, code, and message.

    Args:
        bigdata_response (dict):
            BigDataCorp response.

    Returns:
        list[dict]:
            List of dictionaries with the key, code, and message.
    """
    status_data = bigdata_response.get('Status') or {}
    list_status = []
    for key, value in status_data.items():
        if isinstance(value, list):
            if not value:
                continue
            entry = value[0]
        elif isinstance(value, dict):
            entry = value
        else:
            continue

        code = entry.get('Code')
        if code is None:
            continue
        list_status.append({
            'key': key,
            'code': code,
            'message': entry.get('Message')})
    return list_status


# Simple exceptions, does not require bigdata response
class BigDataCorpAPIException(Exception):
    """Base exception for BigDataCorp API client errors.

    Attributes:
        message (str):
            Human-readable error description.
        payload (dict):
            Optional context returned by the API or client.
    """

    def __repr__(self) -> str:
        """Return the exception representation.

        Returns:
            str:
                Class name and message formatted for display.
        """
        template = "{class_name}: {message}"
        message = self.format_message()
        return template.format(
            class_name=self.__class__.__name__,
            message=message)

    def __str__(self) -> str:
        """Return the string representation of the exception.

        Returns:
            str:
                Same value as ``repr(self)``.
        """
        return self.__repr__()

    def __init__(self, message: str, payload: dict = {}) -> None:
        """Initialize the exception.

        Args:
            message (str):
                Human-readable error description.
            payload (dict):
                Optional context for debugging or logging.
        """
        self.message = message
        self.payload = payload

    def format_message(self) -> str:
        """Build the human-readable message for ``repr`` and ``to_dict``.

        Applies ``str.format`` with ``payload`` only when ``message``
        contains placeholders; fixed map strings are returned as-is.

        Returns:
            str:
                Formatted or raw message. Empty string when ``message`` is
                falsy.
        """
        if not self.message:
            return ""
        if self.payload and "{" in self.message:
            return self.message.format(**self.payload)
        return self.message

    def to_dict(self) -> dict[str, object]:
        """Serialize the exception to a dictionary.

        Returns:
            dict[str, object]:
                Keys ``payload``, ``type``, and ``message``.
        """
        message = self.format_message()
        rv = {
            "payload": self.payload,
            "type": self.__class__.__name__,
            "message": message}
        return rv


class BigDataCorpAPIMaxRetryException(BigDataCorpAPIException):
    """Kept for callers that still catch the old retry error.

    ``send_request`` logs the last transport error and raises that
    error again. It does not raise this class.
    """


class BigDataCorpAPIInvalidDocumentException(BigDataCorpAPIException):
    """Raised when a CPF or CNPJ document is invalid."""


class BigDataCorpAPIInvalidDatabaseException(BigDataCorpAPIException):
    """Raised when the requested dataset is not supported."""


# BigDataCorp specific exceptions associated with specific status codes
class BigDataCorpAPIPayloadStatusException(BigDataCorpAPIException):
    """Payload-status errors with optional API response context in payload.

    Attributes:
        message (str):
            Human-readable error description.
        payload (dict):
            Optional context; ``raise_for_status`` adds ``bigdata_response``.
    """

    STATUS_CODE_MAP: dict[int, str] | None = None
    """Status codes that indicate an error and the corresponding message."""

    @classmethod
    def raise_for_status(cls, bigdata_response: dict,
                         message: str = None, payload: dict = None) -> bool:
        """Raise ``cls`` when a mapped non-zero status code is present.

        Args:
            bigdata_response (dict):
                Parsed BigDataCorp JSON body.
            message (str | None):
                Override for the exception message.
            payload (dict | None):
                Extra context copied into the exception payload.

        Returns:
            bool:
                ``False`` when no mapped error code is found in the response.

        Raises:
            cls:
                When a status ``Code`` matches ``STATUS_CODE_MAP``, or when
                ``STATUS_CODE_MAP`` is ``None`` and ``Code`` is negative.
        """
        if payload is None:
            payload = {}

        status_codes = _extract_status_codes(bigdata_response)
        for status_code in status_codes:
            code = status_code['code']
            default_message = None

            # Check if status are mapped to a specific message
            if cls.STATUS_CODE_MAP is not None:
                if code not in cls.STATUS_CODE_MAP:
                    continue
                default_message = cls.STATUS_CODE_MAP.get(code)

            # If not mapped, do not raise for positive status codes
            elif code >= 0:
                continue
            else:
                template = (
                    "Problema na API da BigDataCorp: código de status não "
                    "mapeado ({code}).")
                default_message = template.format(code=code)

            resolved_message = (
                message or default_message or
                status_code.get('message') or "")
            raise_payload = dict(payload)
            raise_payload['bigdata_response'] = bigdata_response
            raise_payload['bigdata_status'] = status_code
            raise cls(message=resolved_message, payload=raise_payload)
        return False


class BigDataCorpAPIQueryParametersException(BigDataCorpAPIPayloadStatusException):  # NOQA
    """Raised when query parameters are invalid."""

    STATUS_CODE_MAP: dict[int, str] = {
        -113: (
            "Problema na API da BigDataCorp: parâmetro view inválido; "
            "informe uma view existente para o usuário."),
        -114: (
            "Problema na API da BigDataCorp: parâmetro doc inválido."),
        -115: (
            "Problema na API da BigDataCorp: data inválida; verifique "
            "DateFormat."),
        -116: (
            "Problema na API da BigDataCorp: view já existe; altere o "
            "nome e tente novamente."),
        -118: (
            "Problema na API da BigDataCorp: parâmetro email "
            "obrigatório."),
        -120: (
            "Problema na API da BigDataCorp: locale informado não é "
            "suportado."),
        -121: (
            "Problema na API da BigDataCorp: parâmetro phone "
            "obrigatório."),
        -122: (
            "Problema na API da BigDataCorp: parâmetro name "
            "obrigatório."),
        -123: (
            "Problema na API da BigDataCorp: parâmetro birth date "
            "obrigatório."),
        -124: (
            "Problema na API da BigDataCorp: view não encontrada; "
            "informe uma view existente para o usuário."),
        -125: (
            "Problema na API da BigDataCorp: sintaxe do dataset "
            "inválida; consulte a documentação."),
        -126: (
            "Problema na API da BigDataCorp: sintaxe do parâmetro q "
            "inválida; consulte a documentação do DataPlatform."),
        -127: (
            "Problema na API da BigDataCorp: view não habilitada para "
            "esta API; crie uma nova para a API desejada."),
        -130: (
            "Problema na API da BigDataCorp: parâmetros retornaram "
            "resultados demais; refine os campos conforme a "
            "documentação."),
        -131: (
            "Problema na API da BigDataCorp: parâmetro api inválido; "
            "consulte a documentação."),
        -132: (
            "Problema na API da BigDataCorp: parâmetro hooks inválido; "
            "consulte a documentação."),
        -134: (
            "Problema na API da BigDataCorp: não foi possível "
            "interpretar campo(s) de data; informe DateFormat "
            "correto."),
        -137: (
            "Problema na API da BigDataCorp: parâmetro invoice "
            "inválido."),
        -138: (
            "Problema na API da BigDataCorp: parâmetros order "
            "inválidos; verifique property e direction."),
        -139: (
            "Problema na API da BigDataCorp: parâmetro UF "
            "obrigatório."),
        -140: (
            "Problema na API da BigDataCorp: parâmetro RG "
            "obrigatório."),
        -142: (
            "Problema na API da BigDataCorp: parâmetro receipt "
            "inválido."),
        -143: (
            "Problema na API da BigDataCorp: parâmetro de paginação "
            "inválido."),
        -145: (
            "Problema na API da BigDataCorp: parâmetro vehicles "
            "inválido."),
        -146: (
            "Problema na API da BigDataCorp: filtro de idade inválido."),
        -148: (
            "Problema na API da BigDataCorp: parâmetro lawsuits "
            "inválido."),
        -150: (
            "Problema na API da BigDataCorp: parâmetro request_id "
            "inválido."),
        -151: (
            "Problema na API da BigDataCorp: parâmetro entities "
            "inválido."),
        -152: (
            "Problema na API da BigDataCorp: parâmetro croqui "
            "inválido."),
        -153: (
            "Problema na API da BigDataCorp: parâmetro locations "
            "inválido."),
        -154: (
            "Problema na API da BigDataCorp: parâmetro nirfs inválido."),
        -155: (
            "Problema na API da BigDataCorp: parâmetro CAR inválido."),
        -156: (
            "Problema na API da BigDataCorp: parâmetro filesglebes "
            "inválido."),
        -157: (
            "Problema na API da BigDataCorp: raio do polígono grande "
            "demais; use menos de 100 km."),
        -159: (
            "Problema na API da BigDataCorp: limite máximo de "
            "entidades por requisição excedido."),
        -160: (
            "Problema na API da BigDataCorp: corpo da requisição "
            "inválido; verifique o formato do objeto enviado."),
        -161: (
            "Problema na API da BigDataCorp: sintaxe de tags inválida."),
        -162: (
            "Problema na API da BigDataCorp: limite de tags excedido "
            "(máximo 16 por requisição)."),
        -163: (
            "Problema na API da BigDataCorp: tamanho de tag inválido "
            "(chave e valor entre 1 e 255 caracteres)."),
        -164: (
            "Problema na API da BigDataCorp: chave de tag inválida; "
            "use letras ASCII ou sublinhado."),
        -165: (
            "Problema na API da BigDataCorp: valor de tag inválido."),
        -166: (
            "Problema na API da BigDataCorp: parâmetro qlist inválido; "
            "consulte a documentação do DataPlatform."),
        -167: (
            "Problema na API da BigDataCorp: parâmetro timeout "
            "inválido; consulte a documentação."),
    }


class BigDataCorpAPIRestrictedDocumentException(BigDataCorpAPIPayloadStatusException):  # NOQA
    """Raised when a document is restricted."""

    STATUS_CODE_MAP: dict[int, str] = {
        -200: (
            "Problema na API da BigDataCorp: CPF de menor; informe "
            "data de nascimento para processar."),
        -201: (
            "Problema na API da BigDataCorp: CPF de menor (menor de 12 "
            "anos); requisição não permitida por lei."),
        -202: (
            "Problema na API da BigDataCorp: data de nascimento não "
            "confere com o CPF; informe a data correta."),
        -203: (
            "Problema na API da BigDataCorp: CPF de PEP; requisição "
            "não pode ser processada."),
    }


class BigDataCorpAPILoginException(BigDataCorpAPIPayloadStatusException):
    """Raised when authentication fails or the token has expired."""

    STATUS_CODE_MAP: dict[int, str] = {
        -101: (
            "O usuário da plataforma encontra-se expirado, solicitar a "
            "criação de um novo usuário/token para a equipe de suporte."),
        -111: (
            "Problema na API da BigDataCorp: token de acesso inválido; "
            "use o header AccessToken correto."),
        -112: (
            "Problema na API da BigDataCorp: token de acesso não "
            "informado no header da requisição."),
        -129: (
            "Problema na API da BigDataCorp: IP da requisição não "
            "habilitado para este usuário."),
        -168: (
            "Problema na API da BigDataCorp: session id inválido; "
            "gere um novo e tente novamente."),

        # Erros de login
        -1000: (
            "Token de acesso não vinculado à conta DataPlatform."),
        -1001: (
            "Não foi possível abrir a sessão do usuário. Se o erro "
            "persistir, contate a equipe de suporte."),
        -1002: (
            "Não foi possível carregar metadados do DataPlatform. Se o "
            "erro persistir, contate a equipe de suporte."),
        -1003: (
            "Token de acesso expirado. Gere um novo no BDC Center."),
        -1004: (
            "Grupo de usuário desativado. Se não for esperado, contate "
            "a equipe de suporte."),
    }


class BigDataCorpAPIProblemAPIException(BigDataCorpAPIPayloadStatusException):
    """Raised when BigDataCorp reports an internal API or service error."""

    STATUS_CODE_MAP: dict[int, str] = {
        -108: (
            "Problema na API da BigDataCorp: usuário sem permissão para "
            "consultar a API Misc; contate a equipe de suporte."),
        -109: (
            "Problema na API da BigDataCorp: dataset indisponível; "
            "verifique o nome e peça ao administrador habilitar no BDC "
            "Center."),
        -110: (
            "Problema na API da BigDataCorp: dataset inexistente; "
            "verifique o nome informado."),
        -119: (
            "Problema na API da BigDataCorp: não encontrado em cache."),
        -128: (
            "Problema na API da BigDataCorp: limite de consultas do "
            "dataset atingido; peça aumento ao administrador do "
            "domínio."),
        -133: (
            "Problema na API da BigDataCorp: endpoint de hooks não "
            "responde."),
        -135: (
            "Problema na API da BigDataCorp: usuário sem permissão "
            "para a API Products; contate a equipe de suporte."),
        -136: (
            "Problema na API da BigDataCorp: usuário sem permissão "
            "para a API Invoices; contate a equipe de suporte."),
        -141: (
            "Problema na API da BigDataCorp: usuário sem permissão "
            "para a API Receipts; contate a equipe de suporte."),
        -144: (
            "Problema na API da BigDataCorp: usuário sem permissão "
            "para a API Vehicles; contate a equipe de suporte."),
        -147: (
            "Problema na API da BigDataCorp: usuário sem permissão "
            "para a API Lawsuits; contate a equipe de suporte."),
        -149: (
            "Problema na API da BigDataCorp: não foi possível carregar "
            "o arquivo; verifique a URI e tente novamente."),
        -158: (
            "Problema na API da BigDataCorp: usuário sem permissão "
            "para a API Addresses; contate a equipe de suporte."),
        -169: (
            "Problema na API da BigDataCorp: falha ao enviar resultado "
            "via webhooks; verifique limites de payload ou use a Events "
            "API."),
        -204: (
            "Problema na API da BigDataCorp: nenhum resultado para os "
            "parâmetros informados; forneça mais dados."),
        -205: (
            "Problema na API da BigDataCorp: dataset obsoleto; "
            "consulte a documentação e use versão suportada."),
        -1200: (
            "Problema na API da BigDataCorp: erro inesperado. Contate a "
            "equipe de suporte."),
        -1201: (
            "Problema na API da BigDataCorp: dataset muito lento. Tente "
            "novamente em alguns minutos."),
        -1202: (
            "Problema na API da BigDataCorp: credenciais da fonte de "
            "dados inválidas. Contate a equipe de suporte."),
        -1203: (
            "Problema na API da BigDataCorp: dataset desabilitado "
            "temporariamente."),
        -1204: (
            "Problema na API da BigDataCorp: não foi possível criar a "
            "view. Contate a equipe de suporte."),
        -1205: (
            "Problema na API da BigDataCorp: catálogo não encontrado. Se "
            "o erro persistir, contate a equipe de suporte."),
        -1206: (
            "Problema na API da BigDataCorp: contrato não encontrado. Se "
            "o erro persistir, contate a equipe de suporte."),
        -1207: (
            "Problema na API da BigDataCorp: não foi possível carregar o "
            "limite de consulta deste dataset. Se o erro persistir, "
            "contate a equipe de suporte."),
        -1208: (
            "Problema na API da BigDataCorp: não foi possível atualizar "
            "o limite de consulta deste dataset. Se o erro persistir, "
            "contate a equipe de suporte."),
        -1209: (
            "Problema na API da BigDataCorp: não foi possível encontrar "
            "um nome para este número de documento."),
        -1210: (
            "Problema na API da BigDataCorp: não foi possível agregar "
            "trechos a tempo."),
        -1211: (
            "Problema na API da BigDataCorp: não foi possível carregar o "
            "contrato. Se o erro persistir, contate a equipe de suporte."),
        -1212: (
            "Problema na API da BigDataCorp: tempo limite excedido para "
            "esta consulta. Se o erro persistir, contate a equipe de "
            "suporte."),
        -1213: (
            "Problema na API da BigDataCorp: não foi possível conectar ao "
            "recurso parceiro. Se o erro persistir, contate a equipe de "
            "suporte."),
        -1214: (
            "Problema na API da BigDataCorp: muitas requisições. "
            "Implemente rate-limit para evitar erros."),
        -1215: (
            "Problema na API da BigDataCorp: não foi possível agregar os "
            "dados a tempo."),
        -1399: (
            "Problema na API da BigDataCorp: o parâmetro source deve ser "
            "informado."),
    }


class BigDataCorpAPIOnDemandQueriesException(BigDataCorpAPIPayloadStatusException): # NOQA
    """Raised when an on-demand query fails."""

    STATUS_CODE_MAP: dict[int, str] = {
        -2000: (
            "Problema na API da BigDataCorp: não foi possível localizar "
            "arquivos de rede neural para reconhecer captchas."),
        -2001: (
            "Problema na API da BigDataCorp: não foi possível reconhecer "
            "o captcha."),
        -2002: (
            "Problema na API da BigDataCorp: não foi possível processar "
            "a imagem do captcha."),
        -2003: (
            "Problema na API da BigDataCorp: site redirecionado para "
            "uma página desconhecida."),
        -2004: (
            "Problema na API da BigDataCorp: não foi possível baixar a "
            "imagem do captcha."),
        -2005: (
            "Problema na API da BigDataCorp: não foi possível interpretar "
            "os dados retornados."),
        -2006: (
            "Problema na API da BigDataCorp: não foi possível encontrar "
            "o número PIS."),
        -2007: (
            "Problema na API da BigDataCorp: consulta por UF não "
            "implementada; use apenas UFs disponíveis na documentação do "
            "dataset."),
        -2008: (
            "Problema na API da BigDataCorp: dados insuficientes para "
            "gerar o resultado com a entrada informada."),
        -2009: (
            "Problema na API da BigDataCorp: consulta da fonte não "
            "implementada."),
        -2010: (
            "Problema na API da BigDataCorp: a fonte de dados não está "
            "respondendo; tente novamente mais tarde."),
        -2011: (
            "Problema na API da BigDataCorp: CNPJ inativo ou em falência "
            "(resposta da fonte)."),
        -2012: (
            "Problema na API da BigDataCorp: a fonte indicou pendências "
            "no documento, impossibilitando gerar dados."),
    }


class BigDataCorpAPIMonitoringAPIException(BigDataCorpAPIPayloadStatusException):
    """Raised when the monitoring or asynchronous API call fails."""

    STATUS_CODE_MAP: dict[int, str] = {
        -3000: (
            "Problema na API da BigDataCorp: o ID da consulta não "
            "pertence a este grupo de clientes."),
        -3001: (
            "Problema na API da BigDataCorp: o ID da consulta não "
            "existe."),
        -3002: (
            "Problema na API da BigDataCorp: cursor recebido inválido."),
        -3003: (
            "Problema na API da BigDataCorp: URL de assinatura inválida."),
        -3004: (
            "Problema na API da BigDataCorp: ID de assinatura não "
            "encontrado."),
        -3005: (
            "Problema na API da BigDataCorp: não foi possível assinar; "
            "o resultado deve retornar status de sucesso."),
        -3006: (
            "Problema na API da BigDataCorp: não foi possível assinar; "
            "já existe assinatura com estes dados."),
        -3007: (
            "Problema na API da BigDataCorp: não foi possível assinar; "
            "tipo de consulta inválido."),
        -3008: (
            "Problema na API da BigDataCorp: requisição de hook enfileirada, "
            "aguardando envio."),
        -3009: (
            "Problema na API da BigDataCorp: requisição de hook enviada, "
            "aguardando conclusão."),
        -3010: (
            "Problema na API da BigDataCorp: assinatura já está "
            "desabilitada."),
        -3011: (
            "Problema na API da BigDataCorp: usuário da assinatura não "
            "encontrado no banco de dados."),
        -3012: (
            "Problema na API da BigDataCorp: resultado grande demais; "
            "use filtros ou seleção de campos para reduzir a resposta."),
        -3013: (
            "Problema na API da BigDataCorp: múltiplos parâmetros de "
            "hook; use apenas um tipo, conforme a documentação."),
        -3014: (
            "Problema na API da BigDataCorp: alguns datasets solicitados "
            "não são assináveis; consulte a lista na documentação."),
        -3015: (
            "Problema na API da BigDataCorp: limite de assinaturas "
            "atingido; contate a equipe de suporte para aumentar."),
        -3016: (
            "Problema na API da BigDataCorp: assinatura em lote não "
            "habilitada para este usuário; contate a equipe de suporte."),
        -3017: (
            "Problema na API da BigDataCorp: ID de assinatura em lote "
            "não encontrado."),
        -3018: (
            "Problema na API da BigDataCorp: arquivo de entrada inválido; "
            "verifique os critérios na documentação."),
        -3019: (
            "Problema na API da BigDataCorp: parâmetro Datasets inválido; "
            "informe uma configuração válida para esta consulta."),
        -3020: (
            "Problema na API da BigDataCorp: campos monitorados inválidos; "
            "confira o caminho e se o campo está selecionado no dataset."),
    }


class BigDataCorpAPIUnmappedErrorException(BigDataCorpAPIPayloadStatusException): # NOQA
    """Raised when the API returns an unmapped status code."""


PAYLOAD_STATUS_PRIORITY = [
    BigDataCorpAPIQueryParametersException,
    BigDataCorpAPIRestrictedDocumentException,
    BigDataCorpAPILoginException,
    BigDataCorpAPIProblemAPIException,
    BigDataCorpAPIOnDemandQueriesException,
    BigDataCorpAPIMonitoringAPIException,
    BigDataCorpAPIUnmappedErrorException
]
"""Priority list of exceptions to raise for status code."""


def bigdata_raise_for_status(bigdata_response: dict, message: str = None,
                             payload: dict = None) -> bool:
    """Raise the first matching exception for BigDataCorp status codes.

    Args:
        bigdata_response (dict):
            Parsed BigDataCorp JSON body.
        message (str | None):
            Override for the exception message.
        payload (dict | None):
            Extra context copied into the raised exception.

    Returns:
        bool:
            ``False`` when all status codes are zero or absent.

    Raises:
        BigDataCorpAPIPayloadStatusException:
            Subclass matching a mapped code, or
            ``BigDataCorpAPIUnmappedErrorException`` for unmapped negatives.
    """
    if payload is None:
        payload = {}

    for exception in PAYLOAD_STATUS_PRIORITY:
        exception.raise_for_status(
            bigdata_response=bigdata_response, message=message,
            payload=payload)
    return False


RETRY_EXCEPTIONS = (
    BigDataCorpAPIQueryParametersException,
    BigDataCorpAPILoginException,
    BigDataCorpAPIRestrictedDocumentException,
)
"""Domain exceptions that must not be retried."""