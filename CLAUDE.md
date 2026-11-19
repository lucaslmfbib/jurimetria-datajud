# Jurimetria com a API DataJud

App Streamlit para análise jurimétrica com a API pública DataJud (CNJ): consulta por classe, número do processo, tema, parte ou palavras-chave, por tribunal, com gráficos e downloads.

## Estrutura
- `streamlit_app.py`: o app inteiro (consultas, tratamento, gráficos, downloads CSV/TXT).
- `anpp_analysis.py`: análise específica de ANPP (acordo de não persecução penal).
- `GUIA_TRIBUNAIS_E_CODIGOS.md`: siglas de tribunais e códigos de classe; consultar antes de mexer em listas de tribunais.
- `jurimetria_datajud_colab.ipynb`: sobe o app completo no Colab via Cloudflare Tunnel.
- `.streamlit/config.toml`: tema e configurações do Streamlit.

## Como rodar
```bash
pip install -r requirements.txt
export DATAJUD_API_KEY='APIKey SUA_CHAVE'
streamlit run streamlit_app.py --server.port 8520
```
A chave também pode vir de `st.secrets["DATAJUD_API_KEY"]` (Streamlit Cloud) ou ser digitada no app, sempre no formato `APIKey ...`.

## Convenções
- Chave pública do DataJud: ver https://datajud-wiki.cnj.jus.br/api-publica/acesso/. Não commitar.
- Busca por CPF/CNPJ depende do que cada tribunal expõe no endpoint público; não prometer esse resultado na interface.
- O "saldo acumulado" é uma estimativa (proxy), não um dado oficial; manter essa ressalva nos gráficos.
- Não há testes automatizados; validar com consultas reais em um tribunal pequeno.
