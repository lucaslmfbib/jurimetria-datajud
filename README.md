# Jurimetria com a API DataJud

Aplicacao em Streamlit para analise jurimetrica com dados da API publica DataJud.

## Funcionalidades
- Consulta por `classe.codigo` ou por `numeroProcesso`
- Selecao de tribunal por sigla (`tjmg`, `tjsp`, `tjrj`, etc.)
- Tabela de dados e indicadores de resumo
- Graficos:
  - Horario de ajuizamento
  - Expediente x fora do expediente
  - Ajuizamentos mensais
  - Fluxo mensal (ajuizados x atualizados + saldo acumulado proxy)
  - Tempo de tramitacao por orgao (boxplot)
  - Heatmap dia da semana x hora
- Downloads de CSV e TXT

## Requisitos
- Python 3.10+
- Dependencias em `requirements.txt`

## Execucao local
```bash
pip install -r requirements.txt
streamlit run streamlit_app.py --server.port 8520
```

Acesso:
- `http://127.0.0.1:8520`

## Google Colab
- Notebook pronto: `jurimetria_datajud_colab.ipynb`
- Abrir no Colab:
  - https://colab.research.google.com/github/lucaslmfbib/jurimetria-datajud/blob/main/jurimetria_datajud_colab.ipynb

Passos no Colab:
1. Abra o link acima.
2. Execute a celula de instalacao (`pip`).
3. Preencha `API_KEY` no formato `APIKey ...`.
4. Ajuste `TRIBUNAL`, `CLASSE_CODIGO`, `NUMERO_PROCESSO` e `QUANTIDADE`.
5. Rode as celulas de consulta e graficos.

## API Key DataJud
No app, use a chave no formato `APIKey ...`.
Tambem e possivel usar variavel de ambiente:

```bash
export DATAJUD_API_KEY='APIKey SUA_CHAVE'
```

Referencia:
- https://datajud-wiki.cnj.jus.br/api-publica/acesso/

## Publicacao no Streamlit Community Cloud
1. Suba este projeto para o GitHub.
2. Acesse https://share.streamlit.io
3. Clique em `New app`.
4. Selecione o repositorio e o arquivo principal `streamlit_app.py`.
5. Em `Advanced settings > Secrets`, adicione:

```toml
DATAJUD_API_KEY = "APIKey SUA_CHAVE"
```

6. Deploy.

## Autor
- Lucas Martins
- GitHub: https://github.com/lucaslmfbib
- LinkedIn: https://www.linkedin.com/in/lucaslmf/
- Instagram: https://www.instagram.com/lucaslmf_/
