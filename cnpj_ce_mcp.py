#!/usr/bin/env python3
"""
MCP Server: cnpj_ce_mcp

Consulta dados publicos de CNPJ (Receita Federal) filtrados para o Estado
do Ceara. Le de um banco Turso (libSQL hospedado, gratuito) se as variaveis
de ambiente TURSO_DATABASE_URL e TURSO_AUTH_TOKEN estiverem definidas;
caso contrario cai para um arquivo SQLite local (cnpj_ce.db), util para
testar antes de configurar o Turso.

Tabelas disponiveis:
  empresas          - razao social, natureza juridica, capital social, porte
  estabelecimentos  - endereco, situacao cadastral, CNAE, contato (1 por CNPJ completo)
  socios            - quadro societario
  simples           - opcao pelo Simples Nacional / MEI
  cnae, municipio, natureza_juridica, qualificacao_socio, pais, motivo - tabelas de referencia
"""
import asyncio
import os
import sqlite3
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
from fastmcp import FastMCP

mcp = FastMCP("cnpj_ce_mcp")

# ---------------------------------------------------------------------------
# Conexao com o banco (Turso remoto, com fallback para SQLite local)
# ---------------------------------------------------------------------------

_CONN = None

SITUACAO_CADASTRAL_MAP = {
    "01": "NULA",
    "02": "ATIVA",
    "03": "SUSPENSA",
    "04": "INAPTA",
    "08": "BAIXADA",
}

MATRIZ_FILIAL_MAP = {"1": "MATRIZ", "2": "FILIAL"}

IDENTIFICADOR_SOCIO_MAP = {"1": "PESSOA JURIDICA", "2": "PESSOA FISICA", "3": "ESTRANGEIRO"}

FAIXA_ETARIA_MAP = {
    "0": "NAO SE APLICA",
    "1": "0 A 12 ANOS",
    "2": "13 A 20 ANOS",
    "3": "21 A 30 ANOS",
    "4": "31 A 40 ANOS",
    "5": "41 A 50 ANOS",
    "6": "51 A 60 ANOS",
    "7": "61 A 70 ANOS",
    "8": "71 A 80 ANOS",
    "9": "MAIOR QUE 80 ANOS",
}

GROUP_BY_COLUMNS = {
    "municipio": ("es.municipio", "m.descricao", "municipio m ON m.codigo = es.municipio"),
    "cnae": ("es.cnae_fiscal_principal", "c.descricao", "cnae c ON c.codigo = es.cnae_fiscal_principal"),
    "natureza_juridica": ("em.natureza_juridica", "nj.descricao", "natureza_juridica nj ON nj.codigo = em.natureza_juridica"),
    "situacao_cadastral": ("es.situacao_cadastral", None, None),
}

ORDENAR_POR_SQL = {
    "razao_social": "em.razao_social ASC",
    "capital_social_desc": "em.capital_social DESC",
    "capital_social_asc": "em.capital_social ASC",
    "data_inicio_desc": "es.data_inicio_atividade DESC",
    "data_inicio_asc": "es.data_inicio_atividade ASC",
}

REFERENCE_TABLES = {
    "cnae": "cnae",
    "municipio": "municipio",
    "natureza_juridica": "natureza_juridica",
    "qualificacao_socio": "qualificacao_socio",
    "pais": "pais",
    "motivo": "motivo",
}


def _get_conn():
    """Abre (uma unica vez) a conexao com o banco: Turso se configurado, senao SQLite local."""
    global _CONN
    if _CONN is not None:
        return _CONN

    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    if url:
        import libsql
        _CONN = libsql.connect(database=url, auth_token=token)
    else:
        local_path = os.environ.get(
            "LOCAL_DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "cnpj_ce.db")
        )
        if not os.path.exists(local_path):
            raise RuntimeError(
                f"Nenhuma conexao configurada: defina TURSO_DATABASE_URL/TURSO_AUTH_TOKEN, "
                f"ou coloque o arquivo cnpj_ce.db em {local_path}."
            )
        _CONN = sqlite3.connect(local_path, check_same_thread=False)
    return _CONN


def _query_sync(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Executa um SELECT e retorna uma lista de dicts (nome_coluna -> valor)."""
    conn = _get_conn()
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


_DB_LOCK = asyncio.Lock()


async def _query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Roda _query_sync numa thread separada para nao bloquear o event loop
    (o driver libsql/sqlite3 e sincrono; sem isso, uma consulta lenta trava
    o servidor inteiro, inclusive requisicoes de outros clientes). A trava
    serializa o acesso: a conexao e compartilhada e nao e segura para duas
    queries rodarem ao mesmo tempo em threads diferentes (isso ja travou o
    servidor durante os testes)."""
    async with _DB_LOCK:
        return await asyncio.to_thread(_query_sync, sql, params)


def _only_digits(s: str) -> str:
    return "".join(ch for ch in s if ch.isdigit())


def _fts_match_query(text: str) -> Optional[str]:
    """Monta uma query FTS5 a partir de texto livre: AND implicito entre palavras,
    prefixo por palavra (ex: 'vo leonor' -> '"vo"* "leonor"*', acha qualquer linha
    com um token comecando por 'vo' E outro comecando por 'leonor', em qualquer
    ordem). Cada palavra vai entre aspas para blindar contra sintaxe especial do
    FTS5 (: ^ - etc.). Retorna None se nao sobrar nenhum termo valido (ex: texto
    so com pontuacao)."""
    parts = []
    for token in text.split():
        cleaned = token.replace('"', "")
        if cleaned:
            parts.append(f'"{cleaned}"*')
    return " ".join(parts) if parts else None


def _situacao_label(code: Optional[str]) -> Optional[str]:
    if code is None:
        return None
    return SITUACAO_CADASTRAL_MAP.get(code, code)


# Pares (acentuado, base) mais comuns em portugues, para busca insensivel a acento.
_ACCENT_PAIRS = [
    ("Á", "A"), ("À", "A"), ("Â", "A"), ("Ã", "A"), ("Ä", "A"),
    ("É", "E"), ("È", "E"), ("Ê", "E"), ("Ë", "E"),
    ("Í", "I"), ("Ì", "I"), ("Î", "I"), ("Ï", "I"),
    ("Ó", "O"), ("Ò", "O"), ("Ô", "O"), ("Õ", "O"), ("Ö", "O"),
    ("Ú", "U"), ("Ù", "U"), ("Û", "U"), ("Ü", "U"),
    ("Ç", "C"), ("Ñ", "N"),
]


def _strip_accents(text: str) -> str:
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _normalize_search_term(text: str) -> str:
    """Remove acentos e uniformiza para maiusculas, para casar com _norm_sql_expr()."""
    return _strip_accents(text).upper()


def _norm_sql_expr(column_sql: str) -> str:
    """SQL que remove acentos comuns de `column_sql` e uniformiza maiusculas, para
    permitir busca insensivel a acento (ex: buscar 'sao paulo' encontra 'São Paulo')."""
    expr = f"UPPER({column_sql})"
    for accented, base in _ACCENT_PAIRS:
        expr = f"REPLACE({expr}, '{accented}', '{base}')"
    return expr


def _split_codes(v: Optional[str]) -> List[str]:
    """Separa uma lista de codigos por virgula (ex: '1389,1373') em uma lista limpa."""
    if not v:
        return []
    return [c.strip() for c in v.split(",") if c.strip()]


def _in_clause(column_sql: str, codes: List[str]) -> tuple:
    """Monta 'column IN (?,?,...)' (ou 'column = ?' para um unico codigo) e os args."""
    if len(codes) == 1:
        return f"{column_sql} = ?", tuple(codes)
    placeholders = ",".join("?" for _ in codes)
    return f"{column_sql} IN ({placeholders})", tuple(codes)


def _yyyymmdd(v: Optional[str]) -> Optional[str]:
    """Normaliza uma data informada (com ou sem separadores) para o formato
    YYYYMMDD usado nas colunas de data do dataset da RFB."""
    if v is None:
        return None
    digits = _only_digits(v)
    if len(digits) != 8:
        raise ValueError(f"Data deve ter 8 digitos (YYYYMMDD ou YYYY-MM-DD), recebido: '{v}'.")
    return digits


# ---------------------------------------------------------------------------
# Tool 1: consulta por numero de CNPJ
# ---------------------------------------------------------------------------

class ConsultarCnpjInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    cnpj: str = Field(
        ...,
        description="Numero do CNPJ, com ou sem formatacao (ex: '07396865000168' ou '07.396.865/0001-68'). Deve ter 14 digitos.",
        min_length=11,
        max_length=18,
    )

    @field_validator("cnpj")
    @classmethod
    def validate_cnpj(cls, v: str) -> str:
        digits = _only_digits(v)
        if len(digits) != 14:
            raise ValueError(f"CNPJ deve ter 14 digitos numericos, recebido {len(digits)}.")
        return digits


@mcp.tool(
    name="cnpj_consultar",
    annotations={
        "title": "Consultar CNPJ por numero",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def cnpj_consultar(params: ConsultarCnpjInput) -> Dict[str, Any]:
    """Consulta o cadastro completo de um estabelecimento do Ceara pelo numero do CNPJ.

    Retorna dados da empresa (razao social, natureza juridica, capital social, porte),
    do estabelecimento (nome fantasia, endereco, situacao cadastral, atividade
    economica/CNAE, contato), do quadro societario e da opcao pelo Simples
    Nacional/MEI, quando existentes. So encontra CNPJs com pelo menos um
    estabelecimento sediado no Ceara (o dataset foi filtrado para o estado).

    Args:
        params (ConsultarCnpjInput): contem o campo 'cnpj' com o numero completo (14 digitos).

    Returns:
        dict com as chaves:
            - encontrado (bool)
            - estabelecimento (dict | None): dados do estabelecimento e endereco
            - empresa (dict | None): razao social, natureza juridica, capital social, porte
            - socios (list[dict]): quadro societario (nome, qualificacao, data de entrada)
            - simples (dict | None): opcao pelo Simples Nacional e MEI
        Se nao encontrado, retorna {"encontrado": False, "cnpj": "..."} — o CNPJ pode
        nao existir, ou existir mas nao ter estabelecimento no Ceara.
    """
    return await _consultar_cnpj_impl(params.cnpj)


async def _consultar_cnpj_impl(cnpj: str) -> Dict[str, Any]:
    """Logica de cnpj_consultar, extraida para ser reaproveitada por
    cnpj_ficha_completa sem duplicar codigo."""
    rows = await _query(
        """
        SELECT es.*, m.descricao AS municipio_nome, c.descricao AS atividade_principal_descricao
        FROM estabelecimentos es
        LEFT JOIN municipio m ON m.codigo = es.municipio
        LEFT JOIN cnae c ON c.codigo = es.cnae_fiscal_principal
        WHERE es.cnpj = ?
        """,
        (cnpj,),
    )
    if not rows:
        return {"encontrado": False, "cnpj": cnpj, "mensagem": "CNPJ nao encontrado na base do Ceara."}

    estab = rows[0]
    estab["situacao_cadastral_descricao"] = _situacao_label(estab.get("situacao_cadastral"))
    cnpj_basico = estab["cnpj_basico"]

    empresa_rows = await _query("SELECT * FROM empresas WHERE cnpj_basico = ?", (cnpj_basico,))
    empresa = empresa_rows[0] if empresa_rows else None

    socios = await _query(
        """
        SELECT s.*, q.descricao AS qualificacao_descricao
        FROM socios s
        LEFT JOIN qualificacao_socio q ON q.codigo = s.qualificacao_socio
        WHERE s.cnpj_basico = ?
        """,
        (cnpj_basico,),
    )

    simples_rows = await _query("SELECT * FROM simples WHERE cnpj_basico = ?", (cnpj_basico,))
    simples = simples_rows[0] if simples_rows else None

    return {
        "encontrado": True,
        "estabelecimento": estab,
        "empresa": empresa,
        "socios": socios,
        "simples": simples,
    }


# ---------------------------------------------------------------------------
# Tool 2: busca de estabelecimentos (nome, atividade, municipio, situacao)
# ---------------------------------------------------------------------------

_FROM_ESTABELECIMENTOS = """
    FROM estabelecimentos es
    JOIN empresas em ON em.cnpj_basico = es.cnpj_basico
    LEFT JOIN simples si ON si.cnpj_basico = es.cnpj_basico
    LEFT JOIN municipio m ON m.codigo = es.municipio
    LEFT JOIN cnae c ON c.codigo = es.cnae_fiscal_principal
    LEFT JOIN motivo mo ON mo.codigo = es.motivo_situacao_cadastral
"""


def _montar_where_estabelecimentos(p) -> tuple:
    """Monta a clausula WHERE, os args e JOINs extras (FTS5) para os filtros de
    estabelecimentos, compartilhada entre cnpj_buscar_estabelecimentos e
    cnpj_exportar_csv. Retorna (where_sql, args, extra_join_sql)."""
    where = []
    args: list = []
    extra_join: list = []
    # razao_social/nome_fantasia/bairro usam indices FTS5 (fts_empresas,
    # fts_estab_fantasia, fts_estab_bairro; ver mcp/SETUP_TURSO_MCP.md) em vez de
    # LIKE '%...%' — sem eles, essas buscas em tabelas de ~2M linhas levavam
    # 12-28s por nao terem indice utilizavel. tokenize="unicode61 remove_diacritics 2"
    # tambem os torna insensiveis a acento.
    if p.razao_social:
        fts_q = _fts_match_query(p.razao_social)
        if fts_q:
            extra_join.append("JOIN fts_empresas fts_rs ON fts_rs.rowid = em.rowid")
            where.append("fts_rs.razao_social MATCH ?")
            args.append(fts_q)
        else:
            where.append("0")
    if p.nome_fantasia:
        fts_q = _fts_match_query(p.nome_fantasia)
        if fts_q:
            extra_join.append("JOIN fts_estab_fantasia fts_nf ON fts_nf.rowid = es.rowid")
            where.append("fts_nf.nome_fantasia MATCH ?")
            args.append(fts_q)
        else:
            where.append("0")
    if p.bairro:
        fts_q = _fts_match_query(p.bairro)
        if fts_q:
            extra_join.append("JOIN fts_estab_bairro fts_bi ON fts_bi.rowid = es.rowid")
            where.append("fts_bi.bairro MATCH ?")
            args.append(fts_q)
        else:
            where.append("0")
    if p.cep_prefixo:
        where.append("es.cep LIKE ?")
        args.append(f"{_only_digits(p.cep_prefixo)}%")
    if p.municipio_codigo:
        clause, vals = _in_clause("es.municipio", _split_codes(p.municipio_codigo))
        where.append(clause)
        args.extend(vals)
    if p.cnae_codigo:
        clause, vals = _in_clause("es.cnae_fiscal_principal", _split_codes(p.cnae_codigo))
        where.append(clause)
        args.extend(vals)
    if p.cnae_secundario_codigo:
        where.append("(',' || es.cnae_fiscal_secundaria || ',') LIKE ?")
        args.append(f"%,{p.cnae_secundario_codigo},%")
    if p.situacao_cadastral:
        where.append("es.situacao_cadastral = ?")
        args.append(p.situacao_cadastral)
    if p.motivo_situacao_cadastral:
        where.append("es.motivo_situacao_cadastral = ?")
        args.append(p.motivo_situacao_cadastral)
    if p.apenas_matriz is not None:
        where.append("es.identificador_matriz_filial = ?")
        args.append("1" if p.apenas_matriz else "2")
    if p.porte_empresa:
        where.append("em.porte_empresa = ?")
        args.append(p.porte_empresa)
    if p.capital_social_min is not None:
        where.append("em.capital_social >= ?")
        args.append(p.capital_social_min)
    if p.capital_social_max is not None:
        where.append("em.capital_social <= ?")
        args.append(p.capital_social_max)
    if p.data_inicio_de:
        where.append("es.data_inicio_atividade >= ?")
        args.append(p.data_inicio_de)
    if p.data_inicio_ate:
        where.append("es.data_inicio_atividade <= ?")
        args.append(p.data_inicio_ate)
    if p.data_situacao_de:
        where.append("es.data_situacao_cadastral >= ?")
        args.append(p.data_situacao_de)
    if p.data_situacao_ate:
        where.append("es.data_situacao_cadastral <= ?")
        args.append(p.data_situacao_ate)
    if p.opcao_simples is not None:
        where.append("si.opcao_simples = ?")
        args.append("S" if p.opcao_simples else "N")
    if p.opcao_mei is not None:
        where.append("si.opcao_mei = ?")
        args.append("S" if p.opcao_mei else "N")
    if p.tem_situacao_especial is not None:
        if p.tem_situacao_especial:
            where.append("(es.situacao_especial IS NOT NULL AND es.situacao_especial <> '')")
        else:
            where.append("(es.situacao_especial IS NULL OR es.situacao_especial = '')")
    if p.tem_telefone is not None:
        if p.tem_telefone:
            where.append("(es.telefone1 IS NOT NULL AND es.telefone1 <> '')")
        else:
            where.append("(es.telefone1 IS NULL OR es.telefone1 = '')")
    if p.tem_email is not None:
        if p.tem_email:
            where.append("(es.correio_eletronico IS NOT NULL AND es.correio_eletronico <> '')")
        else:
            where.append("(es.correio_eletronico IS NULL OR es.correio_eletronico = '')")
    return " AND ".join(where), args, " ".join(extra_join)


def _tem_algum_filtro_estabelecimentos(p) -> bool:
    return any([
        p.razao_social, p.nome_fantasia, p.municipio_codigo, p.bairro, p.cep_prefixo, p.cnae_codigo,
        p.cnae_secundario_codigo, p.situacao_cadastral, p.motivo_situacao_cadastral,
        p.apenas_matriz is not None, p.porte_empresa,
        p.capital_social_min is not None, p.capital_social_max is not None,
        p.data_inicio_de, p.data_inicio_ate, p.data_situacao_de, p.data_situacao_ate,
        p.opcao_simples is not None, p.opcao_mei is not None, p.tem_situacao_especial is not None,
        p.tem_telefone is not None, p.tem_email is not None,
    ])


class BuscarEstabelecimentosInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    razao_social: Optional[str] = Field(
        default=None, description="Trecho da razao social a buscar (uma ou mais palavras; ignora acentos e maiusculas/minusculas). Ex: 'PADARIA'.", max_length=200
    )
    nome_fantasia: Optional[str] = Field(
        default=None, description="Trecho do nome fantasia a buscar (uma ou mais palavras; ignora acentos e maiusculas/minusculas). Ex: 'VO LEONOR'.", max_length=200
    )
    municipio_codigo: Optional[str] = Field(
        default=None,
        description="Codigo(s) numerico(s) do municipio, separados por virgula para buscar em varios de uma vez "
        "(ex: '1389,1373'). Obtenha com cnpj_referencia_buscar tabela='municipio'.",
        max_length=200,
    )
    bairro: Optional[str] = Field(
        default=None, description="Trecho do bairro a buscar (uma ou mais palavras; ignora acentos e maiusculas/minusculas). Ex: 'ALDEOTA'.", max_length=200
    )
    cep_prefixo: Optional[str] = Field(
        default=None, description="Prefixo do CEP para busca hiperlocal (ex: '60712' encontra todos os CEPs que comecam assim).", max_length=8
    )
    cnae_codigo: Optional[str] = Field(
        default=None,
        description="Codigo(s) de CNAE fiscal principal, separados por virgula para buscar em varios de uma vez "
        "(ex: '4781400,4712100'). Obtenha com cnpj_referencia_buscar tabela='cnae'.",
        max_length=200,
    )
    cnae_secundario_codigo: Optional[str] = Field(
        default=None,
        description="Codigo de CNAE que deve aparecer entre as atividades SECUNDARIAS do estabelecimento (nao o principal).",
        max_length=10,
    )
    situacao_cadastral: Optional[str] = Field(
        default=None, description="Filtra pela situacao cadastral: '01' NULA, '02' ATIVA, '03' SUSPENSA, '04' INAPTA, '08' BAIXADA."
    )
    motivo_situacao_cadastral: Optional[str] = Field(
        default=None,
        description="Codigo do motivo da situacao cadastral (ex: motivo da baixa — incorporacao, omissao de "
        "declaracoes etc.). Obtenha com cnpj_referencia_buscar tabela='motivo'.",
        max_length=10,
    )
    apenas_matriz: Optional[bool] = Field(
        default=None, description="Se True, retorna so matrizes (exclui filiais); se False, retorna so filiais."
    )
    porte_empresa: Optional[str] = Field(
        default=None, description="Filtra pelo porte da empresa: '01' MICRO EMPRESA, '03' EMPRESA DE PEQUENO PORTE, '05' DEMAIS (nao ME/EPP)."
    )
    capital_social_min: Optional[float] = Field(default=None, description="Capital social minimo da empresa (R$), inclusive.", ge=0)
    capital_social_max: Optional[float] = Field(default=None, description="Capital social maximo da empresa (R$), inclusive.", ge=0)
    data_inicio_de: Optional[str] = Field(
        default=None, description="Data de inicio de atividade minima, inclusive (formato 'YYYY-MM-DD' ou 'YYYYMMDD')."
    )
    data_inicio_ate: Optional[str] = Field(
        default=None, description="Data de inicio de atividade maxima, inclusive (formato 'YYYY-MM-DD' ou 'YYYYMMDD')."
    )
    data_situacao_de: Optional[str] = Field(
        default=None,
        description="Data minima da ULTIMA mudanca de situacao cadastral, inclusive (formato 'YYYY-MM-DD' ou "
        "'YYYYMMDD'). Util para achar quem foi baixado/mudou de status num periodo (ex: analise de churn).",
    )
    data_situacao_ate: Optional[str] = Field(
        default=None, description="Data maxima da ULTIMA mudanca de situacao cadastral, inclusive (mesmo formato de data_situacao_de)."
    )
    opcao_simples: Optional[bool] = Field(
        default=None, description="Se True, retorna so quem optou pelo Simples Nacional; se False, so quem NAO optou."
    )
    opcao_mei: Optional[bool] = Field(
        default=None, description="Se True, retorna so MEIs; se False, so quem NAO e MEI."
    )
    tem_situacao_especial: Optional[bool] = Field(
        default=None,
        description="Se True, retorna so estabelecimentos com situacao especial registrada (falencia, recuperacao "
        "judicial, liquidacao, espolio etc.); se False, so quem NAO tem nenhuma.",
    )
    tem_telefone: Optional[bool] = Field(
        default=None, description="Se True, retorna so quem tem telefone cadastrado; se False, so quem NAO tem (qualidade de dado/prospeccao)."
    )
    tem_email: Optional[bool] = Field(
        default=None, description="Se True, retorna so quem tem e-mail cadastrado; se False, so quem NAO tem (qualidade de dado/prospeccao)."
    )
    ordenar_por: str = Field(
        default="razao_social",
        description="Como ordenar os resultados: 'razao_social' (A-Z, padrao), 'capital_social_desc' "
        "(maior capital primeiro), 'capital_social_asc', 'data_inicio_desc' (mais recentes primeiro), "
        "'data_inicio_asc' (mais antigas primeiro).",
    )
    limit: int = Field(default=20, description="Numero maximo de resultados (1-100).", ge=1, le=100)
    offset: int = Field(default=0, description="Quantos resultados pular, para paginacao.", ge=0)

    @field_validator("razao_social", "nome_fantasia", "bairro")
    @classmethod
    def _non_empty(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            return None
        return v

    @field_validator("data_inicio_de", "data_inicio_ate", "data_situacao_de", "data_situacao_ate")
    @classmethod
    def _valida_data(cls, v: Optional[str]) -> Optional[str]:
        return _yyyymmdd(v)

    @field_validator("ordenar_por")
    @classmethod
    def _valida_ordenacao(cls, v: str) -> str:
        if v not in ORDENAR_POR_SQL:
            raise ValueError(f"ordenar_por deve ser uma de: {', '.join(ORDENAR_POR_SQL)}")
        return v


@mcp.tool(
    name="cnpj_buscar_estabelecimentos",
    annotations={
        "title": "Buscar estabelecimentos por nome/atividade/municipio",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def cnpj_buscar_estabelecimentos(params: BuscarEstabelecimentosInput) -> Dict[str, Any]:
    """Busca estabelecimentos do Ceara por razao social, nome fantasia, municipio,
    atividade economica (CNAE) e mais de uma dezena de outros filtros (porte,
    capital social, datas, bairro, CEP, Simples/MEI, matriz/filial, situacao
    especial, telefone/e-mail cadastrado etc.). Pelo menos um filtro deve ser
    informado — caso contrario a lista seria enorme. Util para prospeccao (ex:
    'padarias ativas em Fortaleza') e para localizar o CNPJ de uma empresa quando
    so se sabe o nome.

    A busca por texto (razao_social, nome_fantasia, bairro) usa indice de busca
    completa (FTS5): ignora acentos e maiusculas/minusculas, aceita varias
    palavras (todas precisam aparecer, em qualquer ordem — ex: 'padaria centro'
    acha 'PADARIA E CONFEITARIA DO CENTRO') e cada palavra e tratada como prefixo
    (ex: 'panif' acha 'PANIFICADORA').

    Args:
        params (BuscarEstabelecimentosInput): filtros de busca (todos opcionais,
            exceto que pelo menos um deve ser preenchido), 'ordenar_por' para
            escolher a ordenacao, mais limit/offset para paginacao.

    Returns:
        dict com as chaves:
            - total_encontrado (int): total de resultados que atendem aos filtros
              (pode ser maior que a pagina atual — use offset/limit para paginar)
            - total_retornado (int): quantidade de linhas nesta pagina
            - offset (int)
            - limit (int)
            - resultados (list[dict]): cada item contem cnpj, razao_social, nome_fantasia,
              municipio_nome, atividade_principal_descricao, situacao_cadastral_descricao,
              motivo_situacao_descricao, matriz_filial_descricao,
              logradouro/numero/bairro/cep, telefone1, correio_eletronico, porte_empresa,
              capital_social, data_inicio_atividade, opcao_simples, opcao_mei
        Retorna erro se nenhum filtro for informado.
    """
    p = params
    if not _tem_algum_filtro_estabelecimentos(p):
        return {
            "erro": "Informe pelo menos um filtro (ex: razao_social, nome_fantasia, municipio_codigo, "
            "cnae_codigo, porte_empresa, capital_social_min/max, data_inicio_de/ate, opcao_simples, opcao_mei)."
        }

    where_sql, args, extra_join_sql = _montar_where_estabelecimentos(p)
    from_sql = _FROM_ESTABELECIMENTOS + " " + extra_join_sql
    count_sql = f"SELECT COUNT(*) AS total {from_sql} WHERE {where_sql}"
    order_sql = ORDENAR_POR_SQL[p.ordenar_por]
    sql = f"""
        SELECT es.cnpj, em.razao_social, es.nome_fantasia,
               m.descricao AS municipio_nome, c.descricao AS atividade_principal_descricao,
               es.situacao_cadastral, es.data_situacao_cadastral,
               mo.descricao AS motivo_situacao_descricao,
               es.identificador_matriz_filial, es.logradouro, es.numero, es.bairro, es.cep,
               es.telefone1, es.correio_eletronico, es.situacao_especial,
               em.porte_empresa, em.capital_social, es.data_inicio_atividade,
               si.opcao_simples, si.opcao_mei
        {from_sql}
        WHERE {where_sql}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
    """
    # Sequencial (nao asyncio.gather): a conexao com o banco e compartilhada e nao
    # e segura para uso concorrente por duas threads ao mesmo tempo.
    count_rows = await _query(count_sql, tuple(args))
    total = count_rows[0]["total"] if count_rows else None
    rows = await _query(sql, tuple(args) + (p.limit, p.offset))
    for r in rows:
        r["situacao_cadastral_descricao"] = _situacao_label(r.get("situacao_cadastral"))
        r["matriz_filial_descricao"] = MATRIZ_FILIAL_MAP.get(r.get("identificador_matriz_filial"))

    return {
        "total_encontrado": total,
        "total_retornado": len(rows),
        "offset": p.offset,
        "limit": p.limit,
        "resultados": rows,
    }


# ---------------------------------------------------------------------------
# Tool 3: busca de socios
# ---------------------------------------------------------------------------

class BuscarSociosInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    nome: Optional[str] = Field(default=None, description="Trecho do nome do socio a buscar (busca parcial).", max_length=200)
    cnpj_cpf: Optional[str] = Field(
        default=None, description="CPF (com os 3 primeiros/2 ultimos digitos ocultos, como consta na base publica) ou CNPJ do socio, com ou sem formatacao.", max_length=20
    )
    identificador_socio: Optional[str] = Field(
        default=None, description="Tipo do socio: '1' PESSOA JURIDICA, '2' PESSOA FISICA, '3' ESTRANGEIRO."
    )
    qualificacao_socio_codigo: Optional[str] = Field(
        default=None,
        description="Codigo da qualificacao do socio (ex: so 'Socio-Administrador'). Obtenha com "
        "cnpj_referencia_buscar tabela='qualificacao_socio'.",
        max_length=10,
    )
    faixa_etaria: Optional[str] = Field(
        default=None,
        description="Faixa etaria do socio pessoa fisica: '1' 0-12, '2' 13-20, '3' 21-30, '4' 31-40, '5' 41-50, "
        "'6' 51-60, '7' 61-70, '8' 71-80, '9' maior que 80, '0' nao se aplica.",
    )
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)

    @field_validator("nome")
    @classmethod
    def _non_empty(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            return None
        return v


@mcp.tool(
    name="cnpj_buscar_socios",
    annotations={
        "title": "Buscar socios/quadro societario",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def cnpj_buscar_socios(params: BuscarSociosInput) -> Dict[str, Any]:
    """Busca no quadro societario das empresas do Ceara por nome do socio ou
    CPF/CNPJ, retornando as empresas em que a pessoa/entidade aparece como socia.
    Util para descobrir vinculos societarios de uma pessoa (due diligence,
    conflito de interesse, levantamento patrimonial). Nota: CPFs de socios pessoa
    fisica vem parcialmente ocultos na base publica da Receita Federal
    (ex: '***123456**').

    A busca por nome usa indice de busca completa (FTS5): ignora acentos e
    maiusculas/minusculas, aceita varias palavras (todas precisam aparecer, em
    qualquer ordem) e cada palavra e tratada como prefixo.

    Args:
        params (BuscarSociosInput): nome (busca parcial) e/ou cnpj_cpf e/ou filtros
            de tipo de socio (identificador_socio, qualificacao_socio_codigo,
            faixa_etaria), mais paginacao.

    Returns:
        dict com 'total_encontrado' (int), 'total_retornado' (pagina atual),
        'offset', 'limit' e 'resultados': lista de socios encontrados, cada um com
        nome_socio, cnpj_cpf_socio, qualificacao_descricao, identificador_socio,
        identificador_socio_descricao, faixa_etaria, faixa_etaria_descricao,
        data_entrada_sociedade e a empresa vinculada (cnpj_basico, razao_social).
        Retorna erro se nenhum filtro for informado.
    """
    p = params
    if not any([p.nome, p.cnpj_cpf, p.identificador_socio, p.qualificacao_socio_codigo, p.faixa_etaria]):
        return {"erro": "Informe pelo menos um filtro: 'nome', 'cnpj_cpf', 'identificador_socio', 'qualificacao_socio_codigo' ou 'faixa_etaria'."}

    where = []
    args: list = []
    extra_join = ""
    if p.nome:
        fts_q = _fts_match_query(p.nome)
        if fts_q:
            extra_join = "JOIN fts_socios fts_s ON fts_s.rowid = s.rowid"
            where.append("fts_s.nome_socio MATCH ?")
            args.append(fts_q)
        else:
            where.append("0")
    if p.cnpj_cpf:
        where.append("s.cnpj_cpf_socio LIKE ?")
        args.append(f"%{_only_digits(p.cnpj_cpf) or p.cnpj_cpf}%")
    if p.identificador_socio:
        where.append("s.identificador_socio = ?")
        args.append(p.identificador_socio)
    if p.qualificacao_socio_codigo:
        where.append("s.qualificacao_socio = ?")
        args.append(p.qualificacao_socio_codigo)
    if p.faixa_etaria:
        where.append("s.faixa_etaria = ?")
        args.append(p.faixa_etaria)

    where_sql = " AND ".join(where)
    from_sql = f"""
        FROM socios s
        LEFT JOIN qualificacao_socio q ON q.codigo = s.qualificacao_socio
        JOIN empresas em ON em.cnpj_basico = s.cnpj_basico
        {extra_join}
    """
    count_sql = f"SELECT COUNT(*) AS total {from_sql} WHERE {where_sql}"
    sql = f"""
        SELECT s.nome_socio, s.cnpj_cpf_socio, s.data_entrada_sociedade,
               q.descricao AS qualificacao_descricao, s.identificador_socio, s.faixa_etaria,
               s.cnpj_basico, em.razao_social
        {from_sql}
        WHERE {where_sql}
        ORDER BY s.nome_socio
        LIMIT ? OFFSET ?
    """
    count_rows = await _query(count_sql, tuple(args))
    total = count_rows[0]["total"] if count_rows else None
    rows = await _query(sql, tuple(args) + (p.limit, p.offset))
    for r in rows:
        r["identificador_socio_descricao"] = IDENTIFICADOR_SOCIO_MAP.get(r.get("identificador_socio"))
        r["faixa_etaria_descricao"] = FAIXA_ETARIA_MAP.get(r.get("faixa_etaria"))
    return {
        "total_encontrado": total,
        "total_retornado": len(rows),
        "offset": p.offset,
        "limit": p.limit,
        "resultados": rows,
    }


# ---------------------------------------------------------------------------
# Tool 4: consulta de tabelas de referencia (CNAE, municipio, etc.)
# ---------------------------------------------------------------------------

class ReferenciaBuscarInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    tabela: str = Field(
        ...,
        description="Qual tabela de referencia consultar: 'cnae', 'municipio', 'natureza_juridica', 'qualificacao_socio', 'pais' ou 'motivo'.",
    )
    texto: str = Field(..., description="Trecho do texto a buscar na descricao (ex: 'PANIFICACAO', 'FORTALEZA').", min_length=2, max_length=200)
    limit: int = Field(default=20, ge=1, le=100)

    @field_validator("tabela")
    @classmethod
    def validate_tabela(cls, v: str) -> str:
        if v not in REFERENCE_TABLES:
            raise ValueError(f"tabela deve ser uma de: {', '.join(REFERENCE_TABLES)}")
        return v


@mcp.tool(
    name="cnpj_referencia_buscar",
    annotations={
        "title": "Buscar codigo em tabela de referencia",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def cnpj_referencia_buscar(params: ReferenciaBuscarInput) -> Dict[str, Any]:
    """Busca o codigo correspondente a uma descricao nas tabelas de referencia
    (CNAE/atividade economica, municipio, natureza juridica, qualificacao de
    socio, pais, motivo de situacao cadastral). Use esta ferramenta ANTES de
    'cnpj_buscar_estabelecimentos' quando precisar de um municipio_codigo ou
    cnae_codigo a partir de um nome (ex: descobrir o codigo de 'Fortaleza' ou
    de 'Padaria e confeitaria').

    Args:
        params (ReferenciaBuscarInput): tabela (uma de cnae/municipio/natureza_juridica/
            qualificacao_socio/pais/motivo) e texto (busca parcial na descricao).

    Returns:
        dict com 'tabela' e 'resultados': lista de {"codigo": str, "descricao": str}.
    """
    table = REFERENCE_TABLES[params.tabela]
    rows = await _query(
        f"SELECT codigo, descricao FROM {table} WHERE {_norm_sql_expr('descricao')} LIKE ? ORDER BY descricao LIMIT ?",
        (f"%{_normalize_search_term(params.texto)}%", params.limit),
    )
    return {"tabela": params.tabela, "resultados": rows}


# ---------------------------------------------------------------------------
# Tool 5: estatisticas agregadas
# ---------------------------------------------------------------------------

class EstatisticasInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    agrupar_por: str = Field(
        ..., description="Como agrupar a contagem: 'municipio', 'cnae', 'natureza_juridica' ou 'situacao_cadastral'."
    )
    agrupar_por_2: Optional[str] = Field(
        default=None,
        description="Segunda dimensao para CRUZAR com agrupar_por (ex: municipio + cnae ao mesmo tempo, pra "
        "responder 'quais CNAEs mais aparecem em cada municipio'). Mesmas opcoes de agrupar_por; deve ser "
        "diferente dela. Cuidado: o numero de combinacoes possiveis cresce rapido, 'top' limita quantas voltam.",
    )
    municipio_codigo: Optional[str] = Field(default=None, description="Filtra por municipio antes de agrupar (nao use junto com agrupar_por='municipio').")
    cnae_codigo: Optional[str] = Field(default=None, description="Filtra por CNAE antes de agrupar (nao use junto com agrupar_por='cnae').")
    situacao_cadastral: Optional[str] = Field(default=None, description="Filtra pela situacao cadastral ('01'..'08') antes de agrupar.")
    top: int = Field(default=15, description="Quantas categorias (ou combinacoes, se agrupar_por_2 for usado) retornar, ordenadas pela contagem (maior primeiro).", ge=1, le=100)

    @field_validator("agrupar_por", "agrupar_por_2")
    @classmethod
    def validate_group(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in GROUP_BY_COLUMNS:
            raise ValueError(f"agrupar_por/agrupar_por_2 deve ser uma de: {', '.join(GROUP_BY_COLUMNS)}")
        return v

    @model_validator(mode="after")
    def _valida_dimensoes_diferentes(self):
        if self.agrupar_por_2 is not None and self.agrupar_por_2 == self.agrupar_por:
            raise ValueError("agrupar_por_2 deve ser diferente de agrupar_por.")
        return self


@mcp.tool(
    name="cnpj_estatisticas",
    annotations={
        "title": "Estatisticas agregadas de estabelecimentos",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def cnpj_estatisticas(params: EstatisticasInput) -> Dict[str, Any]:
    """Conta estabelecimentos do Ceara agrupando por municipio, atividade (CNAE),
    natureza juridica ou situacao cadastral, com filtros opcionais. Util para
    perguntas de visao geral, como 'quantas empresas ativas existem em Fortaleza'
    ou 'quais os municipios com mais padarias'.

    Com agrupar_por_2, cruza DUAS dimensoes de uma vez (ex: municipio + cnae, pra
    'quais CNAEs mais aparecem em cada municipio' numa unica chamada).

    Args:
        params (EstatisticasInput): agrupar_por (obrigatorio), agrupar_por_2
            (opcional, para cruzar duas dimensoes) e filtros opcionais
            (municipio_codigo, cnae_codigo, situacao_cadastral), mais 'top' para
            limitar quantas categorias/combinacoes retornar.

    Returns:
        dict com 'agrupado_por', 'agrupado_por_2' (se usado) e 'resultados':
        lista de {"codigo", "descricao", "codigo_2"?, "descricao_2"?, "quantidade"},
        ordenada por quantidade decrescente.
    """
    return await _estatisticas_impl(
        agrupar_por=params.agrupar_por,
        agrupar_por_2=params.agrupar_por_2,
        municipio_codigo=params.municipio_codigo,
        cnae_codigo=params.cnae_codigo,
        situacao_cadastral=params.situacao_cadastral,
        top=params.top,
    )


async def _estatisticas_impl(
    agrupar_por: str,
    agrupar_por_2: Optional[str] = None,
    municipio_codigo: Optional[str] = None,
    cnae_codigo: Optional[str] = None,
    situacao_cadastral: Optional[str] = None,
    top: int = 15,
) -> Dict[str, Any]:
    """Logica de cnpj_estatisticas, extraida para ser reaproveitada por
    cnpj_panorama_setorial sem duplicar codigo."""
    group_col, desc_col, join_clause = GROUP_BY_COLUMNS[agrupar_por]
    group_col2 = desc_col2 = join_clause2 = None
    if agrupar_por_2:
        group_col2, desc_col2, join_clause2 = GROUP_BY_COLUMNS[agrupar_por_2]

    where = []
    args: list = []
    if municipio_codigo:
        where.append("es.municipio = ?")
        args.append(municipio_codigo)
    if cnae_codigo:
        where.append("es.cnae_fiscal_principal = ?")
        args.append(cnae_codigo)
    if situacao_cadastral:
        where.append("es.situacao_cadastral = ?")
        args.append(situacao_cadastral)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    join_sql = ""
    if "natureza_juridica" in (agrupar_por, agrupar_por_2):
        join_sql = "JOIN empresas em ON em.cnpj_basico = es.cnpj_basico"

    select_desc = "NULL AS descricao"
    if join_clause:
        join_sql += f" LEFT JOIN {join_clause}"
        select_desc = f"{desc_col} AS descricao"

    group_by_sql = group_col
    select_extra = ""
    if group_col2:
        select_desc2 = "NULL AS descricao_2"
        if join_clause2:
            join_sql += f" LEFT JOIN {join_clause2}"
            select_desc2 = f"{desc_col2} AS descricao_2"
        select_extra = f", {group_col2} AS codigo_2, {select_desc2}"
        group_by_sql = f"{group_col}, {group_col2}"

    sql = f"""
        SELECT {group_col} AS codigo, {select_desc}{select_extra}, COUNT(*) AS quantidade
        FROM estabelecimentos es
        {join_sql}
        {where_sql}
        GROUP BY {group_by_sql}
        ORDER BY quantidade DESC
        LIMIT ?
    """
    args.append(top)
    rows = await _query(sql, tuple(args))
    if agrupar_por == "situacao_cadastral":
        for r in rows:
            r["descricao"] = _situacao_label(r.get("codigo"))
    if agrupar_por_2 == "situacao_cadastral":
        for r in rows:
            r["descricao_2"] = _situacao_label(r.get("codigo_2"))

    result = {"agrupado_por": agrupar_por, "resultados": rows}
    if agrupar_por_2:
        result["agrupado_por_2"] = agrupar_por_2
    return result


# ---------------------------------------------------------------------------
# Tool 6: exportar estabelecimentos filtrados para CSV
# ---------------------------------------------------------------------------

class ExportarCsvInput(BuscarEstabelecimentosInput):
    limit: int = Field(default=500, description="Numero maximo de linhas a exportar (1-2000).", ge=1, le=2000)


@mcp.tool(
    name="cnpj_exportar_csv",
    annotations={
        "title": "Exportar estabelecimentos filtrados para CSV",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def cnpj_exportar_csv(params: ExportarCsvInput) -> Dict[str, Any]:
    """Gera um CSV pronto (texto separado por virgula, com cabecalho) a partir dos
    mesmos filtros de cnpj_buscar_estabelecimentos — util para pedir uma planilha
    filtrada sem precisar paginar manualmente e montar o arquivo na conversa.
    Aceita ate 2000 linhas por chamada; para bases maiores que isso, combine
    varias chamadas aumentando 'offset' e concatene os CSVs (reaproveitando o
    cabecalho so da primeira).

    Args:
        params (ExportarCsvInput): mesmos filtros de BuscarEstabelecimentosInput
            (razao_social, municipio_codigo, cnae_codigo, porte_empresa,
            capital_social_min/max, data_inicio_de/ate, opcao_simples, opcao_mei
            etc.), com 'limit' ate 2000 em vez de 100.

    Returns:
        dict com 'total_encontrado' (int | None — mesma logica de
        cnpj_buscar_estabelecimentos), 'linhas_exportadas' (int) e 'csv' (str):
        o conteudo CSV pronto, com cabecalho na primeira linha.
        Retorna erro se nenhum filtro for informado.
    """
    p = params
    if not _tem_algum_filtro_estabelecimentos(p):
        return {
            "erro": "Informe pelo menos um filtro (ex: razao_social, nome_fantasia, municipio_codigo, "
            "cnae_codigo, porte_empresa, capital_social_min/max, data_inicio_de/ate, opcao_simples, opcao_mei)."
        }

    where_sql, args, extra_join_sql = _montar_where_estabelecimentos(p)
    from_sql = _FROM_ESTABELECIMENTOS + " " + extra_join_sql
    order_sql = ORDENAR_POR_SQL[p.ordenar_por]
    sql = f"""
        SELECT es.cnpj, em.razao_social, es.nome_fantasia,
               m.descricao AS municipio_nome, c.descricao AS atividade_principal_descricao,
               es.situacao_cadastral, es.data_situacao_cadastral,
               mo.descricao AS motivo_situacao_descricao,
               es.identificador_matriz_filial, es.logradouro, es.numero, es.bairro, es.cep,
               es.telefone1, es.correio_eletronico, es.situacao_especial,
               em.porte_empresa, em.capital_social, es.data_inicio_atividade,
               si.opcao_simples, si.opcao_mei
        {from_sql}
        WHERE {where_sql}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
    """
    count_sql = f"SELECT COUNT(*) AS total {from_sql} WHERE {where_sql}"
    count_rows = await _query(count_sql, tuple(args))
    total = count_rows[0]["total"] if count_rows else None
    rows = await _query(sql, tuple(args) + (p.limit, p.offset))
    for r in rows:
        r["situacao_cadastral_descricao"] = _situacao_label(r.get("situacao_cadastral"))
        r["matriz_filial_descricao"] = MATRIZ_FILIAL_MAP.get(r.get("identificador_matriz_filial"))

    import csv
    import io

    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return {
        "total_encontrado": total,
        "linhas_exportadas": len(rows),
        "csv": buf.getvalue(),
    }


# ---------------------------------------------------------------------------
# Tool 7: ficha completa (consulta + contexto setorial numa chamada so)
# ---------------------------------------------------------------------------

@mcp.tool(
    name="cnpj_ficha_completa",
    annotations={
        "title": "Ficha completa de um CNPJ (com contexto setorial)",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def cnpj_ficha_completa(params: ConsultarCnpjInput) -> Dict[str, Any]:
    """Ficha completa de um CNPJ: tudo que cnpj_consultar traz (empresa,
    estabelecimento, socios, Simples/MEI) MAIS contexto setorial automatico
    (quantas empresas ativas/baixadas/inaptas/etc no mesmo CNAE principal e
    municipio). Use esta ferramenta sempre que o pedido for por "ficha completa",
    "perfil completo" ou "raio-x" de uma empresa — ja inclui a analise que
    normalmente precisaria de uma chamada em cnpj_consultar mais outra em
    cnpj_estatisticas.

    Args:
        params (ConsultarCnpjInput): contem o campo 'cnpj' com o numero completo (14 digitos).

    Returns:
        Mesmas chaves de cnpj_consultar (encontrado, estabelecimento, empresa,
        socios, simples) mais 'contexto_setorial' (dict | None): descricao,
        contagem por situacao cadastral, total e ativas no mesmo CNAE+municipio.
        contexto_setorial vem None se a empresa nao tiver CNAE ou municipio
        registrado (raro).
    """
    base = await _consultar_cnpj_impl(params.cnpj)
    if not base.get("encontrado"):
        return base

    estab = base["estabelecimento"]
    cnae = estab.get("cnae_fiscal_principal")
    municipio = estab.get("municipio")

    contexto = None
    if cnae and municipio:
        stats = await _estatisticas_impl("situacao_cadastral", municipio_codigo=municipio, cnae_codigo=cnae, top=10)
        resultados = stats["resultados"]
        total_setor = sum(r["quantidade"] for r in resultados)
        ativas = next((r["quantidade"] for r in resultados if r["codigo"] == "02"), 0)
        contexto = {
            "descricao": (
                f"Estabelecimentos com o mesmo CNAE principal ({cnae}) no municipio "
                f"{estab.get('municipio_nome') or municipio}"
            ),
            "por_situacao_cadastral": resultados,
            "total_no_setor_local": total_setor,
            "ativas_no_setor_local": ativas,
        }

    base["contexto_setorial"] = contexto
    return base


# ---------------------------------------------------------------------------
# Tool 8: panorama setorial/regional
# ---------------------------------------------------------------------------

class PanoramaSetorialInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    cnae_codigo: Optional[str] = Field(
        default=None, description="Codigo do CNAE para analisar (obtenha com cnpj_referencia_buscar tabela='cnae')."
    )
    municipio_codigo: Optional[str] = Field(
        default=None, description="Codigo do municipio para analisar (obtenha com cnpj_referencia_buscar tabela='municipio')."
    )
    top: int = Field(default=10, description="Quantas categorias retornar em cada ranking (1-50).", ge=1, le=50)

    @model_validator(mode="after")
    def _pelo_menos_um(self):
        if not self.cnae_codigo and not self.municipio_codigo:
            raise ValueError("Informe pelo menos cnae_codigo ou municipio_codigo.")
        return self


@mcp.tool(
    name="cnpj_panorama_setorial",
    annotations={
        "title": "Panorama setorial ou regional",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def cnpj_panorama_setorial(params: PanoramaSetorialInput) -> Dict[str, Any]:
    """Panorama completo de um setor (CNAE) e/ou regiao (municipio) do Ceara:
    situacao cadastral, naturezas juridicas mais comuns entre as ativas, e um
    ranking (top municipios se so cnae_codigo for informado, ou top atividades
    se so municipio_codigo for informado). Use esta ferramenta sempre que o
    pedido for por "panorama setorial", "visao geral do mercado/setor", "como
    esta o setor X" ou "raio-x de uma regiao" — ja combina varias chamadas de
    cnpj_estatisticas numa resposta so. Pode levar alguns segundos a mais que
    uma chamada unica de cnpj_estatisticas, por fazer 3-4 consultas internas.

    Args:
        params (PanoramaSetorialInput): cnae_codigo e/ou municipio_codigo
            (pelo menos um obrigatorio), mais 'top' para os rankings.

    Returns:
        dict com 'filtros' (o que foi analisado), 'por_situacao_cadastral',
        'por_natureza_juridica_ativas', e 'top_municipios_ativas' (so quando
        so cnae_codigo foi informado) ou 'top_atividades_ativas' (so quando so
        municipio_codigo foi informado).
    """
    p = params
    situacao = await _estatisticas_impl(
        "situacao_cadastral", municipio_codigo=p.municipio_codigo, cnae_codigo=p.cnae_codigo, top=10
    )
    natureza = await _estatisticas_impl(
        "natureza_juridica", municipio_codigo=p.municipio_codigo, cnae_codigo=p.cnae_codigo,
        situacao_cadastral="02", top=p.top,
    )

    result: Dict[str, Any] = {
        "filtros": {"cnae_codigo": p.cnae_codigo, "municipio_codigo": p.municipio_codigo},
        "por_situacao_cadastral": situacao["resultados"],
        "por_natureza_juridica_ativas": natureza["resultados"],
    }

    if p.cnae_codigo and not p.municipio_codigo:
        top_mun = await _estatisticas_impl("municipio", cnae_codigo=p.cnae_codigo, situacao_cadastral="02", top=p.top)
        result["top_municipios_ativas"] = top_mun["resultados"]
    if p.municipio_codigo and not p.cnae_codigo:
        top_cnae = await _estatisticas_impl("cnae", municipio_codigo=p.municipio_codigo, situacao_cadastral="02", top=p.top)
        result["top_atividades_ativas"] = top_cnae["resultados"]

    return result


# ---------------------------------------------------------------------------
# Prompts: atalhos para pedidos comuns em texto livre ("ficha completa",
# "panorama setorial") aparecerem como acao rapida na interface do cliente MCP
# (quando o cliente suportar prompts — nem todos exibem essa lista).
# ---------------------------------------------------------------------------

@mcp.prompt(
    name="ficha_completa",
    description="Ficha completa de uma empresa do Ceara (dados cadastrais + contexto setorial) a partir do CNPJ.",
)
def ficha_completa_prompt(cnpj: str) -> str:
    return (
        f"Use a ferramenta cnpj_ficha_completa para consultar o CNPJ {cnpj}. "
        "Apresente o resultado formatado de forma clara (nao como JSON cru): dados da "
        "empresa, do estabelecimento, socios, Simples/MEI, e o contexto setorial "
        "(quantas empresas ativas/baixadas existem no mesmo ramo e municipio)."
    )


@mcp.prompt(
    name="panorama_setorial",
    description="Panorama de um setor (CNAE) e/ou regiao (municipio) do Ceara: situacao cadastral, naturezas juridicas e rankings.",
)
def panorama_setorial_prompt(cnae_codigo: str = "", municipio_codigo: str = "") -> str:
    filtros = []
    if cnae_codigo:
        filtros.append(f"cnae_codigo='{cnae_codigo}'")
    if municipio_codigo:
        filtros.append(f"municipio_codigo='{municipio_codigo}'")
    filtros_txt = ", ".join(filtros) if filtros else "(pergunte ao usuario qual CNAE ou municipio analisar)"
    return (
        f"Use a ferramenta cnpj_panorama_setorial com {filtros_txt}. Se precisar descobrir "
        "o codigo do CNAE ou municipio a partir de um nome, use cnpj_referencia_buscar antes. "
        "Apresente o resultado formatado de forma clara (nao como JSON cru), com uma leitura "
        "interpretativa dos numeros, nao so a tabela."
    )


# ---------------------------------------------------------------------------
# Limitador de requisicoes (protecao leve contra abuso): o servidor e publico
# e sem autenticacao (decisao deliberada, dado que os dados sao publicos), mas
# sem limite qualquer um com a URL poderia consumir a cota gratuita do Turso.
# ---------------------------------------------------------------------------

class RateLimitMiddleware:
    """Limite simples de requisicoes por IP (janela deslizante em memoria).
    Suficiente para um unico processo Render; nao e distribuido nem persistente."""

    def __init__(self, app, max_requests: int = 60, window_seconds: float = 60.0):
        self.app = app
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict = {}

    def _client_ip(self, scope) -> str:
        headers = dict(scope.get("headers") or [])
        fwd = headers.get(b"x-forwarded-for")
        if fwd:
            return fwd.decode("latin-1").split(",")[0].strip()
        client = scope.get("client")
        return client[0] if client else "unknown"

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        import time

        ip = self._client_ip(scope)
        now = time.monotonic()
        hits = [t for t in self._hits.get(ip, []) if now - t < self.window_seconds]
        if len(hits) >= self.max_requests:
            from starlette.responses import JSONResponse

            response = JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32000, "message": "Muitas requisicoes. Tente novamente em instantes."}},
                status_code=429,
            )
            await response(scope, receive, send)
            return
        hits.append(now)
        self._hits[ip] = hits
        await self.app(scope, receive, send)


if __name__ == "__main__":
    port = os.environ.get("PORT")
    if port:
        # Hospedagem remota (ex. Render): expõe via HTTP em vez de stdio.
        from starlette.middleware import Middleware
        from starlette.middleware.cors import CORSMiddleware

        cors = Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["mcp-session-id"],
        )
        rate_limit = Middleware(RateLimitMiddleware, max_requests=60, window_seconds=60.0)
        mcp.run(transport="http", host="0.0.0.0", port=int(port), path="/mcp", middleware=[cors, rate_limit])
    else:
        mcp.run()
