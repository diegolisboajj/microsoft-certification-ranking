# 🏆 Microsoft Certification Ranking

Ranking automatizado dos profissionais com mais certificações Microsoft no mundo, organizado por país e continente. Os dados são coletados diariamente via API do [Credly](https://www.credly.com) e publicados neste repositório.

## 📋 Rankings Disponíveis

| Arquivo | Escopo |
|---|---|
| [MS_TOP10_BRAZIL.md](./MS_TOP10_BRAZIL.md) | 🇧🇷 Top 10 – Brasil |
| [MS_TOP10_AMERICAS.md](./MS_TOP10_AMERICAS.md) | 🗽 Top 10 – Américas |
| [MS_TOP10_EUROPE.md](./MS_TOP10_EUROPE.md) | 🇪🇺 Top 10 – Europa |
| [MS_TOP10_ASIA.md](./MS_TOP10_ASIA.md) | 🌏 Top 10 – Ásia |
| [MS_TOP10_AFRICA.md](./MS_TOP10_AFRICA.md) | 🦁 Top 10 – África |
| [MS_TOP10_OCEANIA.md](./MS_TOP10_OCEANIA.md) | 🌊 Top 10 – Oceania |
| [MS_TOP10_WORLD.md](./MS_TOP10_WORLD.md) | 🌍 Top 10 – Global |
| [MS_TOP10_BRAZIL_COMMUNITY.md](./MS_TOP10_BRAZIL_COMMUNITY.md) | 🇧🇷 Top 10 – Comunidade Brasil |

Cada ranking exibe o **Top 10 profissionais** por número de certificações, com empates agrupados na mesma posição, além do **Top 5 empresas** com mais certificados e **estatísticas gerais** (total de usuários, total de badges e média por usuário).

## ⚙️ Como Funciona

### Coleta de Dados

1. **`fetch_ms_country.py`** — Busca certificações de um único país via API do Credly, utilizando o diretório do GitHub como pool de usuários (ID da org: `63074953-290b-4dce-86ce-ea04b4187219`). Filtra apenas badges emitidos pela Microsoft (ID oficial no Credly: `1392f199-abe0-4698-92b5-834610af6baf`).

2. **`fetch_large_ms_country.py`** — Versão paralelizada para países com grande volume de usuários (Brasil, Índia, EUA, Reino Unido etc.), escalando até 100 páginas de resultados.

3. **`fetch_all_ms_countries.py`** — Orquestrador que executa a coleta para todos os ~198 países de forma assíncrona, invocando os scripts acima como subprocessos.

4. **`known_missing_users.json`** — Lista manual de usuários conhecidos que não aparecem no diretório padrão do Credly. Eles são injetados forçadamente no pipeline para garantir representação correta no ranking.

### Geração dos Rankings

5. **`generate_ms_rankings.py`** — Lê todos os CSVs gerados em `datasource_ms/`, consolida os dados por usuário (deduplicando badges pelo nome normalizado), aplica o mapeamento geográfico por continente e gera os 7 arquivos Markdown de ranking.

6. **`generate_ms_brazil_community.py`** — Gera o ranking da comunidade brasileira, incluindo badges de programas parceiros Microsoft (Power Up, Certiport).

### Armazenamento

Os dados brutos ficam em `datasource_ms/`, com um arquivo CSV por país no formato `ms-certs-<country-name>.csv`. Cada linha representa um usuário com seu nome, número de badges e lista de certificações.

## 🤖 Atualização Automática

O workflow `.github/workflows/generate-ms-rankings.yml` executa o pipeline completo **todos os dias às 01:00 UTC** (ou manualmente via `workflow_dispatch`). Ao final, commita automaticamente os arquivos Markdown e CSVs atualizados no repositório.

```
Fetch All Countries → Generate Rankings → Commit & Push
```

## 🛠️ Execução Local

```bash
# Instalar dependências
python3 -m venv venv
source venv/bin/activate
pip install requests

# Buscar um único país
python3 fetch_ms_country.py "Brazil"

# Buscar país de grande volume (paralelizado)
python3 fetch_large_ms_country.py "Brazil"

# Buscar todos os países (processo longo)
python3 fetch_all_ms_countries.py

# Gerar os rankings a partir dos CSVs
python3 generate_ms_rankings.py
```

## 📁 Estrutura do Projeto

```
.
├── datasource_ms/               # CSVs com dados brutos por país
├── .github/workflows/           # Pipeline CI/CD (GitHub Actions)
├── fetch_ms_country.py          # Coleta para países padrão
├── fetch_large_ms_country.py    # Coleta para países de alto volume
├── fetch_all_ms_countries.py    # Orquestrador global
├── generate_ms_rankings.py      # Gerador dos rankings por região
├── generate_ms_brazil_community.py  # Ranking da comunidade brasileira
├── known_missing_users.json     # Usuários injetados manualmente
├── ms_csv_metadata.json         # Metadados dos CSVs gerados
├── MS_TOP10_*.md                # Rankings gerados (um por região)
└── PROJECT_MS_GUIDELINES.md     # Guia técnico do projeto
```

## 🔍 Regras do Ranking

- Apenas certificações **ativas e não expiradas** são contabilizadas.
- Badges com nomes equivalentes são **deduplicados** por normalização do nome (remoção de prefixos, pontuação e variações).
- Em caso de **empate**, os usuários compartilham a mesma posição.
- Cada posição pode exibir no máximo 20 usuários empatados.
