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
import os
import sqlite3
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, ConfigDict, field_validator
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

GROUP_BY_COLUMNS = {
    "municipio": ("es.municipio", "m.descricao", "municipio m ON m.codigo = es.municipio"),
    "cnae": ("es.cnae_fiscal_principal", "c.descricao", "cnae c ON c.codigo = es.cnae_fiscal_principal"),
    "natureza_juridica": ("em.natureza_juridica", "nj.descricao", "natureza_juridica nj ON nj.codigo = em.natureza_juridica"),
    "situacao_cadastral": ("es.situacao_cadastral", None, None),
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


def _query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """Executa um SELECT e retorna uma lista de dicts (nome_coluna -> valor)."""
    conn = _get_conn()
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _only_digits(s: str) -> str:
    return "".join(ch for ch in s if ch.isdigit())


def _situacao_label(code: Optional[str]) -> Optional[str]:
    if code is None:
        return None
    return SITUACAO_CADASTRAL_MAP.get(code, code)


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
    cnpj = params.cnpj
    rows = _query(
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

    empresa_rows = _query("SELECT * FROM empresas WHERE cnpj_basico = ?", (cnpj_basico,))
    empresa = empresa_rows[0] if empresa_rows else None

    socios = _query(
        """
        SELECT s.*, q.descricao AS qualificacao_descricao
        FROM socios s
        LEFT JOIN qualificacao_socio q ON q.codigo = s.qualificacao_socio
        WHERE s.cnpj_basico = ?
        """,
        (cnpj_basico,),
    )

    simples_rows = _query("SELECT * FROM simples WHERE cnpj_basico = ?", (cnpj_basico,))
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

class BuscarEstabelecimentosInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    razao_social: Optional[str] = Field(
        default=None, description="Trecho da razao social a buscar (busca parcial, sem diferenciar maiusculas). Ex: 'PADARIA'.", max_length=200
    )
    nome_fantasia: Optional[str] = Field(
        default=None, description="Trecho do nome fantasia a buscar (busca parcial). Ex: 'VO LEONOR'.", max_length=200
    )
    municipio_codigo: Optional[str] = Field(
        default=None, description="Codigo numerico do municipio (obtenha com cnpj_referencia_buscar tabela='municipio').", max_length=10
    )
    cnae_codigo: Optional[str] = Field(
        default=None, description="Codigo do CNAE fiscal principal (obtenha com cnpj_referencia_buscar tabela='cnae').", max_length=10
    )
    situacao_cadastral: Optional[str] = Field(
        default=None, description="Filtra pela situacao cadastral: '01' NULA, '02' ATIVA, '03' SUSPENSA, '04' INAPTA, '08' BAIXADA."
    )
    limit: int = Field(default=20, description="Numero maximo de resultados (1-100).", ge=1, le=100)
    offset: int = Field(default=0, description="Quantos resultados pular, para paginacao.", ge=0)

    @field_validator("razao_social", "nome_fantasia")
    @classmethod
    def _non_empty(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            return None
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
    atividade economica (CNAE) e/ou situacao cadastral. Pelo menos um filtro de
    texto (razao_social ou nome_fantasia) ou codigo (municipio_codigo/cnae_codigo)
    deve ser informado — caso contrario a lista seria enorme. Util para prospeccao
    (ex: 'padarias ativas em Fortaleza') e para localizar o CNPJ de uma empresa
    quando so se sabe o nome.

    Args:
        params (BuscarEstabelecimentosInput): filtros de busca (todos opcionais,
            exceto que pelo menos um deve ser preenchido), mais limit/offset para paginacao.

    Returns:
        dict com as chaves:
            - total_retornado (int): quantidade de linhas nesta pagina
            - offset (int)
            - limit (int)
            - resultados (list[dict]): cada item contem cnpj, razao_social, nome_fantasia,
              municipio_nome, atividade_principal_descricao, situacao_cadastral_descricao,
              logradouro/numero/bairro/cep, telefone1, correio_eletronico
        Retorna erro se nenhum filtro for informado.
    """
    p = params
    if not any([p.razao_social, p.nome_fantasia, p.municipio_codigo, p.cnae_codigo]):
        return {
            "erro": "Informe pelo menos um filtro: razao_social, nome_fantasia, municipio_codigo ou cnae_codigo."
        }

    where = []
    args: list = []
    if p.razao_social:
        where.append("em.razao_social LIKE ? COLLATE NOCASE")
        args.append(f"%{p.razao_social}%")
    if p.nome_fantasia:
        where.append("es.nome_fantasia LIKE ? COLLATE NOCASE")
        args.append(f"%{p.nome_fantasia}%")
    if p.municipio_codigo:
        where.append("es.municipio = ?")
        args.append(p.municipio_codigo)
    if p.cnae_codigo:
        where.append("es.cnae_fiscal_principal = ?")
        args.append(p.cnae_codigo)
    if p.situacao_cadastral:
        where.append("es.situacao_cadastral = ?")
        args.append(p.situacao_cadastral)

    where_sql = " AND ".join(where)
    sql = f"""
        SELECT es.cnpj, em.razao_social, es.nome_fantasia,
               m.descricao AS municipio_nome, c.descricao AS atividade_principal_descricao,
               es.situacao_cadastral, es.logradouro, es.numero, es.bairro, es.cep,
               es.telefone1, es.correio_eletronico
        FROM estabelecimentos es
        JOIN empresas em ON em.cnpj_basico = es.cnpj_basico
        LEFT JOIN municipio m ON m.codigo = es.municipio
        LEFT JOIN cnae c ON c.codigo = es.cnae_fiscal_principal
        WHERE {where_sql}
        ORDER BY em.razao_social
        LIMIT ? OFFSET ?
    """
    args.extend([p.limit, p.offset])
    rows = _query(sql, tuple(args))
    for r in rows:
        r["situacao_cadastral_descricao"] = _situacao_label(r.get("situacao_cadastral"))

    return {"total_retornado": len(rows), "offset": p.offset, "limit": p.limit, "resultados": rows}


# ---------------------------------------------------------------------------
# Tool 3: busca de socios
# ---------------------------------------------------------------------------

class BuscarSociosInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    nome: Optional[str] = Field(default=None, description="Trecho do nome do socio a buscar (busca parcial).", max_length=200)
    cnpj_cpf: Optional[str] = Field(
        default=None, description="CPF (com os 3 primeiros/2 ultimos digitos ocultos, como consta na base publica) ou CNPJ do socio, com ou sem formatacao.", max_length=20
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

    Args:
        params (BuscarSociosInput): nome (busca parcial) e/ou cnpj_cpf, mais paginacao.

    Returns:
        dict com 'total_retornado', 'offset', 'limit' e 'resultados': lista de
        socios encontrados, cada um com nome_socio, cnpj_cpf_socio,
        qualificacao_descricao, data_entrada_sociedade e a empresa vinculada
        (cnpj_basico, razao_social).
        Retorna erro se nem nome nem cnpj_cpf forem informados.
    """
    p = params
    if not p.nome and not p.cnpj_cpf:
        return {"erro": "Informe 'nome' ou 'cnpj_cpf' para buscar."}

    where = []
    args: list = []
    if p.nome:
        where.append("s.nome_socio LIKE ? COLLATE NOCASE")
        args.append(f"%{p.nome}%")
    if p.cnpj_cpf:
        where.append("s.cnpj_cpf_socio LIKE ?")
        args.append(f"%{_only_digits(p.cnpj_cpf) or p.cnpj_cpf}%")

    where_sql = " AND ".join(where)
    sql = f"""
        SELECT s.nome_socio, s.cnpj_cpf_socio, s.data_entrada_sociedade,
               q.descricao AS qualificacao_descricao,
               s.cnpj_basico, em.razao_social
        FROM socios s
        LEFT JOIN qualificacao_socio q ON q.codigo = s.qualificacao_socio
        JOIN empresas em ON em.cnpj_basico = s.cnpj_basico
        WHERE {where_sql}
        ORDER BY s.nome_socio
        LIMIT ? OFFSET ?
    """
    args.extend([p.limit, p.offset])
    rows = _query(sql, tuple(args))
    return {"total_retornado": len(rows), "offset": p.offset, "limit": p.limit, "resultados": rows}


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
    rows = _query(
        f"SELECT codigo, descricao FROM {table} WHERE descricao LIKE ? COLLATE NOCASE ORDER BY descricao LIMIT ?",
        (f"%{params.texto}%", params.limit),
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
    municipio_codigo: Optional[str] = Field(default=None, description="Filtra por municipio antes de agrupar (nao use junto com agrupar_por='municipio').")
    cnae_codigo: Optional[str] = Field(default=None, description="Filtra por CNAE antes de agrupar (nao use junto com agrupar_por='cnae').")
    situacao_cadastral: Optional[str] = Field(default=None, description="Filtra pela situacao cadastral ('01'..'08') antes de agrupar.")
    top: int = Field(default=15, description="Quantas categorias retornar, ordenadas pela contagem (maior primeiro).", ge=1, le=100)

    @field_validator("agrupar_por")
    @classmethod
    def validate_group(cls, v: str) -> str:
        if v not in GROUP_BY_COLUMNS:
            raise ValueError(f"agrupar_por deve ser uma de: {', '.join(GROUP_BY_COLUMNS)}")
        return v


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

    Args:
        params (EstatisticasInput): agrupar_por (obrigatorio) e filtros opcionais
            (municipio_codigo, cnae_codigo, situacao_cadastral), mais 'top' para
            limitar quantas categorias retornar.

    Returns:
        dict com 'agrupado_por' e 'resultados': lista de
        {"codigo": str, "descricao": str | None, "quantidade": int},
        ordenada por quantidade decrescente.
    """
    p = params
    group_col, desc_col, join_clause = GROUP_BY_COLUMNS[p.agrupar_por]

    where = []
    args: list = []
    if p.municipio_codigo:
        where.append("es.municipio = ?")
        args.append(p.municipio_codigo)
    if p.cnae_codigo:
        where.append("es.cnae_fiscal_principal = ?")
        args.append(p.cnae_codigo)
    if p.situacao_cadastral:
        where.append("es.situacao_cadastral = ?")
        args.append(p.situacao_cadastral)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    join_sql = ""
    select_desc = "NULL AS descricao"
    if p.agrupar_por == "natureza_juridica":
        join_sql = "JOIN empresas em ON em.cnpj_basico = es.cnpj_basico"
    if join_clause:
        join_sql += f" LEFT JOIN {join_clause}"
        select_desc = f"{desc_col} AS descricao"

    sql = f"""
        SELECT {group_col} AS codigo, {select_desc}, COUNT(*) AS quantidade
        FROM estabelecimentos es
        {join_sql}
        {where_sql}
        GROUP BY {group_col}
        ORDER BY quantidade DESC
        LIMIT ?
    """
    args.append(p.top)
    rows = _query(sql, tuple(args))
    if p.agrupar_por == "situacao_cadastral":
        for r in rows:
            r["descricao"] = _situacao_label(r.get("codigo"))
    return {"agrupado_por": p.agrupar_por, "resultados": rows}


if __name__ == "__main__":
    port = os.environ.get("PORT")
    if port:
        # Hospedagem remota (ex. Render): expõe via HTTP em vez de stdio.
        mcp.run(transport="http", host="0.0.0.0", port=int(port), path="/mcp")
    else:
        mcp.run()
