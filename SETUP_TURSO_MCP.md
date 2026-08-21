# Publicando o banco do Ceará no Turso e ligando o MCP

Este guia parte de onde a extração parou: o banco SQLite completo
(`cnpj_ce.db`, ~1 GB) é montado localmente a partir dos CSVs já filtrados em
`dados\2026-07-12\CE\`, depois enviado para o Turso (hospedagem gratuita,
5 GB), e por fim o servidor MCP (`mcp\cnpj_ce_mcp.py`) é ligado ao Claude
Desktop apontando para ele.

O CLI do Turso não tem instalador nativo para Windows — só é documentado via
WSL (Windows Subsystem for Linux). Os passos abaixo assumem WSL com Ubuntu.

## 1. Montar o banco SQLite local

No PowerShell, dentro da pasta do projeto:

```powershell
cd "C:\SISTEMAS\CONSULTOR CNPJ\dados\2026-07-12\CE"
python _build_db.py
```

Se `python` não estiver no PATH, use o interpretador do `.venv` do projeto:

```powershell
& "C:\SISTEMAS\CONSULTOR CNPJ\.venv\Scripts\python.exe" _build_db.py
```

Isso gera `cnpj_ce.db` (~1 GB) na própria pasta `CE`. Leva menos de 2 minutos.

## 2. Instalar WSL (se ainda não tiver)

No PowerShell **como Administrador**:

```powershell
wsl --install
```

Reinicie se for pedido. Na primeira abertura do Ubuntu, crie um usuário/senha
Linux (só local, não precisa ser o mesmo do Windows).

## 3. Instalar e configurar o Turso CLI (dentro do WSL/Ubuntu)

Abra o terminal Ubuntu (menu Iniciar → Ubuntu) e rode:

```bash
curl -sSfL https://get.tur.so/install.sh | bash
export PATH="$PATH:$HOME/.turso"
```

Login (abre uma URL — copie e cole no navegador do Windows se ele não abrir
sozinho):

```bash
turso auth login
```

## 4. Enviar o banco para o Turso

Dentro do WSL, os drives do Windows ficam em `/mnt/c/...`. Vá até a pasta e
crie o banco a partir do arquivo local (isso faz o upload):

```bash
cd "/mnt/c/SISTEMAS/CONSULTOR CNPJ/dados/2026-07-12/CE"
turso db create cnpj-ce --from-file cnpj_ce.db --location gru
```

(`gru` = São Paulo, a região mais próxima do Brasil disponível no Turso.)

Pegue a URL de conexão e gere um token:

```bash
turso db show cnpj-ce --url
turso db tokens create cnpj-ce
```

Guarde os dois valores — são o `TURSO_DATABASE_URL` (algo como
`libsql://cnpj-ce-seu-usuario.turso.io`) e o `TURSO_AUTH_TOKEN`.

## 5. Instalar as dependências Python do MCP

No PowerShell, na pasta `mcp`:

```powershell
cd "C:\SISTEMAS\CONSULTOR CNPJ\mcp"
pip install -r requirements.txt
```

(ou `& "C:\SISTEMAS\CONSULTOR CNPJ\.venv\Scripts\pip.exe" install -r requirements.txt`
se preferir usar o `.venv` do projeto)

## 6. Testar sem o Turso primeiro (opcional, recomendado)

Antes de mexer com Turso, dá pra confirmar que o servidor funciona copiando o
`cnpj_ce.db` para dentro da pasta `mcp` — sem definir `TURSO_DATABASE_URL`, o
servidor usa esse arquivo local automaticamente.

## 7. Ligar ao Claude Desktop

Edite (ou crie) `%APPDATA%\Claude\claude_desktop_config.json` e adicione:

```json
{
  "mcpServers": {
    "cnpj_ce": {
      "command": "C:\\SISTEMAS\\CONSULTOR CNPJ\\.venv\\Scripts\\python.exe",
      "args": ["C:\\SISTEMAS\\CONSULTOR CNPJ\\mcp\\cnpj_ce_mcp.py"],
      "env": {
        "TURSO_DATABASE_URL": "libsql://cnpj-ce-seu-usuario.turso.io",
        "TURSO_AUTH_TOKEN": "cole_o_token_aqui"
      }
    }
  }
}
```

Ajuste o caminho do `command` se não usar o `.venv` do projeto (pode ser
`python.exe` puro, se estiver no PATH). Reinicie o Claude Desktop.

## Ferramentas disponíveis no MCP

- `cnpj_consultar` — perfil completo (empresa + estabelecimento + sócios + Simples/MEI) a partir do número do CNPJ.
- `cnpj_buscar_estabelecimentos` — busca por razão social, nome fantasia, município e/ou CNAE.
- `cnpj_buscar_socios` — busca por nome ou CPF/CNPJ de sócio (útil para levantar vínculos societários).
- `cnpj_referencia_buscar` — encontra o código de um município, CNAE, natureza jurídica etc. a partir do nome.
- `cnpj_estatisticas` — contagens agrupadas (por município, atividade, natureza jurídica, situação cadastral).

## Sobre os limites do plano gratuito do Turso

5 GB de armazenamento total, sem pausa por inatividade, até 100 bancos, 500
milhões de leituras/mês e 10 milhões de escritas/mês. O banco do Ceará
(~1 GB) cabe com folga.
