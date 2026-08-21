# cnpj_ce_mcp

Servidor [MCP](https://modelcontextprotocol.io) que expõe o dataset público
de CNPJ da Receita Federal, recortado para o Estado do Ceará (~2 milhões de
estabelecimentos), como um conjunto de ferramentas invocáveis por um LLM.
Hospedado como serviço HTTP remoto — não é um script local que o cliente
precisa gerenciar.

**Servidor em produção**: `https://consultacnpj-12lf.onrender.com/mcp`

## Índice

- [Arquitetura](#arquitetura)
- [Modelo de dados](#modelo-de-dados)
- [Catálogo de ferramentas](#catálogo-de-ferramentas)
- [Busca textual (FTS5)](#busca-textual-fts5)
- [Concorrência e acesso ao banco](#concorrência-e-acesso-ao-banco)
- [Deploy](#deploy)
- [Rodando localmente](#rodando-localmente)
- [Decisões de design e problemas resolvidos](#decisões-de-design-e-problemas-resolvidos)
- [Limitações conhecidas](#limitações-conhecidas)
- [Licença e fonte dos dados](#licença-e-fonte-dos-dados)

## Arquitetura

```
Cliente MCP (Claude, etc.)
        │  JSON-RPC sobre HTTP (streamable-http), 1 sessão = 1 conexão SSE
        ▼
┌─────────────────────────────────────────────┐
│  cnpj_ce_mcp.py  (FastMCP 3.x)               │
│  ┌─────────────────────────────────────────┐ │
│  │ CORSMiddleware   (permite conectores     │ │
│  │                   web tipo claude.ai)    │ │
│  ├─────────────────────────────────────────┤ │
│  │ RateLimitMiddleware (60 req/min por IP,  │ │
│  │                   janela deslizante,     │ │
│  │                   em memória)            │ │
│  ├─────────────────────────────────────────┤ │
│  │ 8 tools + 2 prompts (ver catálogo)       │ │
│  │ asyncio.Lock serializa acesso ao driver  │ │
│  │ asyncio.to_thread() tira I/O do loop     │ │
│  └─────────────────────────────────────────┘ │
└───────────────────┬───────────────────────────┘
                     │ libsql (protocolo Hrana/HTTP)
                     ▼
┌─────────────────────────────────────────────┐
│  Turso (libSQL gerenciado, plano free)       │
│  cnpj-ce.turso.io — SQLite completo,         │
│  incl. tabelas virtuais FTS5                 │
└─────────────────────────────────────────────┘
```

- **Runtime**: Python 3.12, [`fastmcp`](https://gofastmcp.com) (não
  confundir com `mcp.server.fastmcp` — esse módulo foi removido do SDK
  oficial `mcp>=2.0`; usamos o pacote standalone, que traz `mcp==1.29.0`
  como dependência transitiva).
- **Transporte**: `streamable-http` em produção (`PORT` setado pelo Render
  → dispara o branch HTTP em `if __name__ == "__main__"`); `stdio` quando
  rodado localmente sem `PORT` (uso via `claude_desktop_config.json`
  clássico).
- **Banco**: Turso (libSQL — fork de SQLite com replicação/hospedagem
  gerenciada). Fallback para SQLite local (`cnpj_ce.db` no mesmo diretório)
  quando `TURSO_DATABASE_URL`/`TURSO_AUTH_TOKEN` não estão setados — útil
  para desenvolvimento sem depender da rede.
- **Deploy**: Render (web service, free tier, `render.yaml` na raiz).
  Dorme após ~15 min de inatividade; cold start ~30-50s.

## Modelo de dados

Schema idêntico ao layout de distribuição da Receita Federal, com um
recorte geográfico aplicado na carga (só CNPJs com pelo menos um
estabelecimento no Ceará).

| Tabela | Linhas (snapshot 2026-07-12) | Papel |
|---|---:|---|
| `empresas` | 1.931.819 | Nível CNPJ básico (8 dígitos): razão social, natureza jurídica, capital social, porte |
| `estabelecimentos` | 2.003.406 | Nível CNPJ completo (14 dígitos): endereço, contato, CNAE, situação cadastral, matriz/filial |
| `socios` | 658.024 | Quadro societário |
| `simples` | 1.393.330 | Opção pelo Simples Nacional / MEI, por CNPJ básico |
| `cnae`, `municipio`, `natureza_juridica`, `qualificacao_socio`, `pais`, `motivo` | — | Tabelas de referência código → descrição |

Chave de junção: `empresas.cnpj_basico` = `estabelecimentos.cnpj_basico` =
`socios.cnpj_basico` = `simples.cnpj_basico` (os 8 primeiros dígitos do
CNPJ). `capital_social` e `porte_empresa` são atributos de **empresa**, não
de estabelecimento — uma rede com N filiais replica o mesmo valor em todas.

### Índices

B-tree convencionais em todas as colunas usadas em filtro exato (`municipio`,
`cnae_fiscal_principal`, `situacao_cadastral`, `cnpj`, `cnpj_basico` em
`estabelecimentos`; `nome_socio`, `cnpj_cpf_socio`, `cnpj_basico` em
`socios`; `razao_social` em `empresas`).

Tabelas virtuais **FTS5** (`tokenize="unicode61 remove_diacritics 2"`) para
todo campo de busca textual livre, com `rowid` espelhando 1:1 o rowid da
tabela de origem (join direto, sem trigger de sincronização — ver
[Busca textual](#busca-textual-fts5)):

`fts_empresas` (razão social) · `fts_estab_fantasia` (nome fantasia) ·
`fts_estab_bairro` (bairro) · `fts_socios` (nome do sócio) · `fts_cnae` ·
`fts_municipio` · `fts_natureza_juridica` · `fts_qualificacao_socio` ·
`fts_pais` · `fts_motivo` (as seis últimas para as tabelas de referência).

## Catálogo de ferramentas

| Ferramenta | Parâmetros-chave | Retorno |
|---|---|---|
| `cnpj_consultar` | `cnpj` | Ficha completa: estabelecimento + empresa + sócios + Simples/MEI |
| `cnpj_ficha_completa` | `cnpj` | `cnpj_consultar` + contexto setorial (situação cadastral do mesmo CNAE+município) |
| `cnpj_buscar_estabelecimentos` | 20+ filtros combináveis (ver tabela abaixo) + `ordenar_por`, `limit`, `offset` | Página de resultados + `total_encontrado` |
| `cnpj_buscar_socios` | `nome`, `cnpj_cpf`, `identificador_socio`, `qualificacao_socio_codigo`, `faixa_etaria` | Sócios/PJ vinculados, com a(s) empresa(s) |
| `cnpj_referencia_buscar` | `tabela`, `texto` | Código a partir de nome (município, CNAE, natureza jurídica, etc.) |
| `cnpj_estatisticas` | `agrupar_por`, `agrupar_por_2` (cruzamento), filtros, `top` | Contagens agregadas, ordenadas desc |
| `cnpj_panorama_setorial` | `cnae_codigo` e/ou `municipio_codigo`, `top` | Composição de 3-4 chamadas de `cnpj_estatisticas` num panorama único |
| `cnpj_exportar_csv` | Mesmos filtros de `cnpj_buscar_estabelecimentos`, `limit` até 2000 | CSV como string, pronto para salvar |

Filtros de `cnpj_buscar_estabelecimentos` / `cnpj_exportar_csv`:

```
razao_social, nome_fantasia, bairro          → FTS5, multi-palavra, sem acento
municipio_codigo, cnae_codigo                → aceitam lista separada por vírgula
cep_prefixo                                  → prefixo de CEP
cnae_secundario_codigo                       → busca dentro da lista de CNAEs secundários
situacao_cadastral, motivo_situacao_cadastral
apenas_matriz                                → bool
porte_empresa
capital_social_min / capital_social_max
data_inicio_de / data_inicio_ate             → janela de abertura
data_situacao_de / data_situacao_ate         → janela da última mudança de situação
opcao_simples, opcao_mei, tem_situacao_especial, tem_telefone, tem_email → bool
ordenar_por                                  → razao_social | capital_social_desc/asc | data_inicio_desc/asc
```

Dois **prompts MCP** (`ficha_completa`, `panorama_setorial`) ficam
registrados via `@mcp.prompt` como atalho de interface, quando o cliente
suporta a listagem `prompts/list`.

## Busca textual (FTS5)

A primeira versão da busca usava `LIKE '%termo%' COLLATE NOCASE`. Dois
problemas motivaram a migração para FTS5:

1. **Performance**: `LIKE` com wildcard à esquerda não usa índice B-tree —
   full scan em tabelas de ~2M linhas, 12-28s por busca (pior ainda somando
   uma segunda varredura para `COUNT(*)` do total).
2. **Acento**: uma tentativa inicial de normalizar acento via cadeia de
   `REPLACE()` aninhados esbarrou em dois limites do SQLite: (a)
   `UPPER()`/`LOWER()` só tratam ASCII — sem a extensão ICU (que o Turso não
   tem), `ç`/`ã` minúsculos nunca viram maiúsculos; (b) uma cadeia de ~46
   `REPLACE()` aninhados (necessária pra cobrir maiúscula+minúscula de cada
   acento) estourava o limite de profundidade do parser SQL do Turso
   (`parser overflowed its stack`).

FTS5 com tokenizador `unicode61 remove_diacritics 2` resolve os dois de
uma vez: índice invertido (rápido) + normalização Unicode nativa (correta
para qualquer caixa). Custo: manutenção das tabelas `fts_*` precisa ser
refeita manualmente após qualquer recarga de dados (não há trigger de
sincronização — ver script de migração abaixo).

Query FTS5 é montada em `_fts_match_query()`: cada palavra do termo de busca
vira um token `"palavra"*` (prefixo, entre aspas para blindar contra sintaxe
especial do FTS5); múltiplas palavras são combinadas com AND implícito.

Recriar os índices FTS5 após uma recarga de dados:

```sql
CREATE VIRTUAL TABLE fts_empresas USING fts5(razao_social, tokenize="unicode61 remove_diacritics 2");
INSERT INTO fts_empresas(rowid, razao_social) SELECT rowid, razao_social FROM empresas;
-- repetir para fts_estab_fantasia (nome_fantasia, filtrando não-vazio),
-- fts_estab_bairro (bairro, filtrando não-vazio), fts_socios (nome_socio),
-- e as 6 tabelas de referência (fts_cnae, fts_municipio, fts_natureza_juridica,
-- fts_qualificacao_socio, fts_pais, fts_motivo)
```

## Concorrência e acesso ao banco

O driver `libsql` é síncrono. Duas decisões garantem que isso não vire
gargalo nem race condition num servidor `asyncio`:

- **`asyncio.to_thread()`** em toda chamada ao driver (`_query_sync` →
  `_query`), para não bloquear o event loop — sem isso, uma consulta lenta
  de um cliente trava **todas** as requisições de todos os clientes,
  inclusive handshakes que nem tocam o banco.
- **`asyncio.Lock()`** global em torno de cada `_query()`, serializando o
  acesso: a conexão libsql é compartilhada entre threads e **não é segura
  para duas queries simultâneas** — isso já travou o servidor em teste
  (duas chamadas concorrentes na mesma conexão, uma delas nunca retorna).
  O lock não reintroduz o problema original porque cada query individual
  ainda roda em thread separada — o que ele impede é *duas* rodando ao
  mesmo tempo, não uma bloqueando as outras indefinidamente.

Corolário prático: **nunca** use `asyncio.gather()` para paralelizar duas
chamadas a `_query()` dentro do mesmo handler — isso também já causou
travamento em teste. Sequencial, mesmo que pareça desperdiçar paralelismo,
é o padrão correto aqui.

## Deploy

1. **Banco**: montado localmente a partir dos CSVs da Receita via
   `dados/<data>/CE/_build_db.py` (fora deste diretório), depois enviado
   pro Turso via CLI (`turso db create --from-file`). Detalhes completos,
   incluindo os problemas de I/O do WSL2 contornados no processo, em
   [`SETUP_TURSO_MCP.md`](./SETUP_TURSO_MCP.md).
2. **Código**: push em `main` neste repositório → deploy automático no
   Render (`render.yaml` define build/start command).
3. **Variáveis de ambiente** (setadas no painel do Render, não no repo):
   `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN`.

## Rodando localmente

```bash
pip install -r requirements.txt

# Modo stdio (sem PORT) — para uso com claude_desktop_config.json clássico:
python cnpj_ce_mcp.py

# Modo HTTP local (simula produção):
PORT=8500 TURSO_DATABASE_URL=... TURSO_AUTH_TOKEN=... python cnpj_ce_mcp.py
# ou, sem Turso, com um cnpj_ce.db local no mesmo diretório:
PORT=8500 python cnpj_ce_mcp.py
```

Teste rápido de um tool call via HTTP (protocolo MCP streamable-http exige
`initialize` → capturar `mcp-session-id` do header → `notifications/initialized`
→ chamada real):

```bash
curl -s -X POST http://127.0.0.1:8500/mcp \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  --data-raw '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' \
  -D - -o /dev/null | grep -i mcp-session-id
```

## Decisões de design e problemas resolvidos

Histórico condensado, na ordem em que apareceram — útil para não
redescobrir os mesmos problemas numa próxima rodada de mudanças:

- **`mcp.server.fastmcp` não existe mais** no SDK `mcp>=2.0`. Migrado para
  o pacote standalone `fastmcp` (`from fastmcp import FastMCP`), que fixa
  `mcp==1.29.0` como dependência.
- **`libsql.connect()`** usa o parâmetro `database=`, não `url=` — API
  mudou entre versões do pacote `libsql` sem aviso claro no erro (`connect()
  got an unexpected keyword argument`).
- **CORS obrigatório** para conectores web (`claude.ai`): sem
  `CORSMiddleware`, o preflight `OPTIONS` retorna 405 e o navegador bloqueia
  a chamada real antes dela sair — sintoma no cliente é "não foi possível
  alcançar o servidor" mesmo com o servidor de pé e saudável.
- **Bloqueio do event loop / concorrência do driver** — ver seção anterior.
- **`total_encontrado` sempre calculado** hoje (uma query `COUNT(*)`
  adicional por busca). Antes da migração para FTS5, esse `COUNT` era
  condicionalmente pulado para buscas de texto livre (dobraria uma varredura
  já lenta) — hoje que a varredura é indexada, o custo do `COUNT` é
  desprezível e a lógica condicional foi removida.
- **Rate limiting em memória**, não distribuído — suficiente para um único
  processo Render; se o serviço escalar para múltiplas instâncias, deixa de
  proteger de forma consistente (precisaria de um contador externo, tipo
  Redis).

## Limitações conhecidas

- Cobertura geográfica só Ceará — filiais de empresas cearenses fora do
  estado não aparecem.
- Snapshot estático (2026-07-12), sem atualização incremental automatizada.
- CPF de sócio pessoa física parcialmente mascarado, como consta na base
  pública da Receita (não é um recorte nosso).
- Sem autenticação — decisão deliberada dado que os dados são públicos;
  risco limitado a consumo da cota gratuita do Turso, mitigado pelo rate
  limit.
- Índices FTS5 não têm sincronização automática — uma recarga de dados
  (`recarga_mensal.py`, fora deste repo) exige recriar as tabelas `fts_*`
  manualmente.

## Licença e fonte dos dados

Dados públicos de CNPJ da Receita Federal do Brasil
([dados.gov.br](https://dados.gov.br)), sem tratamento de sigilo além do já
aplicado pela própria Receita na publicação (CPF de sócio mascarado).
