import io
import importlib
import os
import time
from typing import Any

import pandas as pd
import requests
from requests import HTTPError
import streamlit as st

URL_PADRAO = "https://api-publica.datajud.cnj.jus.br/api_publica_tjmg/_search"
URL_TEMPLATE = "https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search"


@st.cache_resource(show_spinner=False)
def get_plt() -> Any:
    # Evita custo de import no boot inicial do app.
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mplconfig")
    return importlib.import_module("matplotlib.pyplot")


def normalize_api_key(raw_key: str) -> str:
    key = (raw_key or "").strip().strip("\"'")
    if key.lower().startswith("authorization:"):
        key = key.split(":", 1)[1].strip()
    if not key:
        return ""
    if key.startswith("APIKey ") or key.startswith("Bearer "):
        return key
    return f"APIKey {key}"


def build_url(tribunal_sigla: str) -> str:
    tribunal = (tribunal_sigla or "tjmg").strip().lower()
    return URL_TEMPLATE.format(tribunal=tribunal)


def normalize_numero_processo(raw_numero: str) -> str:
    numero = (raw_numero or "").strip()
    somente_digitos = "".join(ch for ch in numero if ch.isdigit())
    return somente_digitos or numero


def to_sao_paulo_datetime(value: Any) -> Any:
    if value is None or value == "":
        return pd.NaT
    return pd.to_datetime(value, utc=True, errors="coerce").tz_convert("America/Sao_Paulo")


def parse_assuntos(assuntos: Any) -> list[str]:
    if not isinstance(assuntos, list):
        return []
    result: list[str] = []
    for assunto in assuntos:
        if isinstance(assunto, dict):
            result.append(str(assunto.get("nome", "")))
        else:
            result.append(str(assunto))
    return result


def parse_movimentos(movimentos: Any) -> list[list[Any]]:
    if not isinstance(movimentos, list):
        return []

    parsed: list[list[Any]] = []
    for movimento in movimentos:
        if not isinstance(movimento, dict):
            continue
        codigo = movimento.get("codigo")
        nome = movimento.get("nome")
        data_hora = to_sao_paulo_datetime(movimento.get("dataHora"))
        parsed.append([codigo, nome, data_hora])
    return parsed


@st.cache_data(show_spinner=False, ttl=1200)
def fetch_hits(
    api_key: str,
    classe_codigo: int,
    size: int,
    url: str,
    numero_processo: str = "",
) -> list[dict[str, Any]]:
    numero_limpo = normalize_numero_processo(numero_processo)
    if numero_limpo:
        query: dict[str, Any] = {"match": {"numeroProcesso": numero_limpo}}
    else:
        query = {"match": {"classe.codigo": classe_codigo}}

    payload = {
        "size": size,
        "query": query,
        "sort": [{"dataAjuizamento": {"order": "desc"}}],
    }
    headers = {
        "Authorization": normalize_api_key(api_key),
        "Content-Type": "application/json",
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()
    return data.get("hits", {}).get("hits", [])


@st.cache_data(show_spinner=False, ttl=1200)
def hits_to_dataframe(hits: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[list[Any]] = []

    for hit in hits:
        source = hit.get("_source", {}) if isinstance(hit, dict) else {}
        classe = source.get("classe", {}) if isinstance(source.get("classe"), dict) else {}
        orgao = (
            source.get("orgaoJulgador", {})
            if isinstance(source.get("orgaoJulgador"), dict)
            else {}
        )

        rows.append(
            [
                source.get("numeroProcesso"),
                classe.get("nome"),
                source.get("dataAjuizamento"),
                source.get("dataHoraUltimaAtualizacao"),
                source.get("formato"),
                source.get("numeroProcesso"),
                orgao.get("nome"),
                orgao.get("codigoMunicipioIBGE"),
                source.get("grau"),
                source.get("assuntos", []),
                source.get("movimentos", []),
                hit.get("sort") if isinstance(hit, dict) else None,
            ]
        )

    columns = [
        "numero_processo",
        "classe",
        "data_ajuizamento",
        "ultima_atualizacao",
        "formato",
        "codigo",
        "orgao_julgador",
        "municipio",
        "grau",
        "assuntos",
        "movimentos",
        "sort",
    ]

    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df

    df["assuntos"] = df["assuntos"].apply(parse_assuntos)
    df["movimentos"] = df["movimentos"].apply(parse_movimentos)
    df["movimentos"] = df["movimentos"].apply(
        lambda x: sorted(x, key=lambda tup: tup[2] if len(tup) > 2 else pd.NaT, reverse=True)
    )
    df["data_ajuizamento"] = df["data_ajuizamento"].apply(to_sao_paulo_datetime)
    df["ultima_atualizacao"] = df["ultima_atualizacao"].apply(to_sao_paulo_datetime)
    return df


def build_top_100(df_anpp: pd.DataFrame) -> pd.Series:
    if df_anpp.empty:
        return pd.Series(dtype="int64")
    return (
        df_anpp.groupby(["municipio", "orgao_julgador"])["codigo"]
        .count()
        .sort_values(ascending=False)
        .head(100)
    )


def monthly_counts(df_anpp: pd.DataFrame, max_meses: int = 12) -> pd.Series:
    if "data_ajuizamento" not in df_anpp.columns:
        return pd.Series(dtype="int64")

    datas = df_anpp["data_ajuizamento"].dropna()
    if datas.empty:
        return pd.Series(dtype="int64")

    if getattr(datas.dt, "tz", None) is not None:
        # Evita ambiguidades de horario de verao ao reagrupar por mes.
        datas = datas.dt.tz_convert("UTC").dt.tz_localize(None)

    return (
        datas.to_frame(name="data_ajuizamento")
        .set_index("data_ajuizamento")
        .resample("ME")
        .size()
        .sort_index()
        .tail(max_meses)
    )


def _month_counts_from_series(datas: pd.Series, max_meses: int = 12) -> pd.Series:
    serie = datas.dropna()
    if serie.empty:
        return pd.Series(dtype="int64")
    if getattr(serie.dt, "tz", None) is not None:
        serie = serie.dt.tz_convert("UTC").dt.tz_localize(None)
    return (
        serie.to_frame(name="data")
        .set_index("data")
        .resample("ME")
        .size()
        .sort_index()
        .tail(max_meses)
    )


def top_100_to_dataframe(top_100: pd.Series) -> pd.DataFrame:
    if isinstance(top_100.index, pd.MultiIndex) and top_100.index.nlevels >= 2:
        top_100_df = top_100.reset_index(name="quantidade")
        top_100_df.columns = ["municipio", "orgao_julgador", "quantidade"]
        return top_100_df

    top_100_df = top_100.reset_index(name="quantidade")
    if top_100_df.shape[1] == 2:
        top_100_df.columns = ["chave", "quantidade"]
    return top_100_df


def dataframe_for_display(df_anpp: pd.DataFrame) -> pd.DataFrame:
    if df_anpp.empty:
        return df_anpp

    df_view = df_anpp.copy()
    df_view["assuntos"] = df_view["assuntos"].apply(
        lambda x: ", ".join(x[:3]) + (" ..." if len(x) > 3 else "")
        if isinstance(x, list)
        else ""
    )
    df_view["qtd_movimentos"] = df_view["movimentos"].apply(
        lambda x: len(x) if isinstance(x, list) else 0
    )
    return df_view.drop(columns=["movimentos", "sort"], errors="ignore")


def fig_horario(df_anpp: pd.DataFrame) -> Any:
    plt = get_plt()
    contagem = df_anpp["data_ajuizamento"].dt.hour.value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(10, 4))
    contagem.plot(kind="bar", color="skyblue", ax=ax)
    ax.set_title("Horario de ajuizamento dos registros")
    ax.set_xlabel("Hora")
    ax.set_ylabel("Numero de ajuizamentos")
    ax.grid(axis="y", alpha=0.8)
    return fig


def fig_pizza(df_anpp: pd.DataFrame) -> Any:
    plt = get_plt()
    contagem = df_anpp["data_ajuizamento"].dt.hour.value_counts().sort_index()
    ajuizamentos_expediente = contagem[8:19].sum()
    ajuizamentos_fora = contagem[0:8].sum() + contagem[19:].sum()

    labels = ["Das 9h as 19h", "Fora do expediente"]
    sizes = [ajuizamentos_expediente, ajuizamentos_fora]
    colors = ["lightblue", "lightgreen"]

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.pie(
        sizes,
        explode=(0.1, 0),
        labels=labels,
        colors=colors,
        autopct="%1.2f%%",
        startangle=45,
    )
    ax.set_title("Ajuizamento: expediente x fora")
    ax.axis("equal")
    return fig


def fig_mensal(df_anpp: pd.DataFrame) -> Any:
    plt = get_plt()
    max_meses = 12
    df_resampled = monthly_counts(df_anpp, max_meses=max_meses)
    if df_resampled.empty:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.set_title(f"Ajuizamentos - ultimos {max_meses} meses")
        ax.text(0.5, 0.5, "Sem dados para grafico mensal.", ha="center", va="center")
        ax.axis("off")
        return fig

    n_meses = len(df_resampled)

    posicoes = list(range(len(df_resampled)))
    labels = [idx.strftime("%m/%Y") for idx in df_resampled.index]

    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.bar(posicoes, df_resampled.values, color="#4E79A7", alpha=0.9, width=0.65)
    ax.set_xlabel("Meses")
    ax.set_ylabel("Quantidade")
    if n_meses < max_meses:
        ax.set_title(f"Ajuizamentos - ultimos {n_meses} meses disponiveis")
    else:
        ax.set_title(f"Ajuizamentos - ultimos {max_meses} meses")
    ax.set_xticks(posicoes)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(axis="y", linestyle="--", alpha=0.35)

    for i, valor in enumerate(df_resampled.values):
        ax.text(i, valor + 0.5, str(int(valor)), ha="center", va="bottom", fontsize=9)

    fig.tight_layout()
    return fig


def fig_fluxo_mensal(df_anpp: pd.DataFrame, max_meses: int = 12) -> Any:
    plt = get_plt()
    ajuizados = _month_counts_from_series(df_anpp["data_ajuizamento"], max_meses=max_meses)
    atualizados = _month_counts_from_series(df_anpp["ultima_atualizacao"], max_meses=max_meses)

    if ajuizados.empty and atualizados.empty:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.set_title("Fluxo mensal")
        ax.text(0.5, 0.5, "Sem dados para fluxo mensal.", ha="center", va="center")
        ax.axis("off")
        return fig

    idx = ajuizados.index.union(atualizados.index).sort_values()
    ajuizados = ajuizados.reindex(idx, fill_value=0)
    atualizados = atualizados.reindex(idx, fill_value=0)
    saldo = (ajuizados - atualizados).cumsum()

    posicoes = list(range(len(idx)))
    labels = [i.strftime("%m/%Y") for i in idx]

    fig, ax1 = plt.subplots(figsize=(11, 4.4))
    ax1.plot(posicoes, ajuizados.values, color="#4E79A7", marker="o", label="Ajuizados")
    ax1.plot(posicoes, atualizados.values, color="#E15759", marker="o", label="Atualizados")
    ax1.set_ylabel("Quantidade por mes")
    ax1.set_xlabel("Meses")
    ax1.set_xticks(posicoes)
    ax1.set_xticklabels(labels, rotation=45, ha="right")
    ax1.grid(axis="y", linestyle="--", alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(
        posicoes,
        saldo.values,
        color="#59A14F",
        linestyle="--",
        linewidth=2,
        label="Saldo acumulado (proxy)",
    )
    ax2.set_ylabel("Saldo acumulado")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    ax1.set_title("Fluxo mensal (ajuizados x atualizados)")
    fig.tight_layout()
    return fig


def fig_tempo_tramitacao_boxplot(df_anpp: pd.DataFrame, max_orgaos: int = 8) -> Any:
    plt = get_plt()
    base = df_anpp[["orgao_julgador", "data_ajuizamento", "ultima_atualizacao"]].dropna()
    if base.empty:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.set_title("Tempo de tramitacao por orgao")
        ax.text(0.5, 0.5, "Sem dados suficientes.", ha="center", va="center")
        ax.axis("off")
        return fig

    aju = base["data_ajuizamento"]
    atu = base["ultima_atualizacao"]
    if getattr(aju.dt, "tz", None) is not None:
        aju = aju.dt.tz_convert("UTC").dt.tz_localize(None)
    if getattr(atu.dt, "tz", None) is not None:
        atu = atu.dt.tz_convert("UTC").dt.tz_localize(None)

    base = base.assign(dias=(atu - aju).dt.total_seconds() / 86400.0)
    base = base[(base["dias"] >= 0) & (base["dias"].notna())]
    if base.empty:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.set_title("Tempo de tramitacao por orgao")
        ax.text(0.5, 0.5, "Sem dados validos de tempo.", ha="center", va="center")
        ax.axis("off")
        return fig

    limite = base["dias"].quantile(0.99)
    base = base[base["dias"] <= limite]

    top_orgaos = (
        base["orgao_julgador"].value_counts().head(max_orgaos).index.tolist()
    )
    base = base[base["orgao_julgador"].isin(top_orgaos)]
    if base.empty:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.set_title("Tempo de tramitacao por orgao")
        ax.text(0.5, 0.5, "Sem dados para os orgaos selecionados.", ha="center", va="center")
        ax.axis("off")
        return fig

    grupos = []
    labels = []
    for orgao in top_orgaos:
        valores = base.loc[base["orgao_julgador"] == orgao, "dias"].values
        if len(valores) > 0:
            grupos.append(valores)
            labels.append(orgao if len(orgao) <= 28 else orgao[:28] + "...")

    fig, ax = plt.subplots(figsize=(11, 4.8))
    bp = ax.boxplot(grupos, patch_artist=True, showfliers=False)
    for box in bp["boxes"]:
        box.set_facecolor("#A0CBE8")
        box.set_alpha(0.9)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Dias")
    ax.set_title("Tempo entre ajuizamento e ultima atualizacao (Top orgaos)")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    return fig


def fig_heatmap_dia_hora(df_anpp: pd.DataFrame) -> Any:
    plt = get_plt()
    datas = df_anpp["data_ajuizamento"].dropna()
    if datas.empty:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.set_title("Heatmap dia x hora")
        ax.text(0.5, 0.5, "Sem dados para heatmap.", ha="center", va="center")
        ax.axis("off")
        return fig

    tabela = pd.crosstab(datas.dt.dayofweek, datas.dt.hour)
    tabela = tabela.reindex(index=range(7), columns=range(24), fill_value=0)

    fig, ax = plt.subplots(figsize=(11, 3.8))
    im = ax.imshow(tabela.values, aspect="auto", cmap="YlGnBu")
    ax.set_title("Heatmap de ajuizamentos (dia da semana x hora)")
    ax.set_xlabel("Hora")
    ax.set_ylabel("Dia da semana")
    ax.set_xticks(range(0, 24, 2))
    ax.set_yticks(range(7))
    ax.set_yticklabels(["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"])
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="Quantidade")
    fig.tight_layout()
    return fig


def save_outputs(df: pd.DataFrame, top_100: pd.Series) -> None:
    plt = get_plt()
    df.to_csv("consulta_datajud.csv", sep=",", header=True, index=False)

    with open("movimentos_datajud.txt", "w") as file:
        file.write("Arquivo gerado pelo Streamlit.")

    with open("top_100_datajud.txt", "w") as file:
        for index, value in top_100.items():
            file.write(f"{index[0]} | {index[1]} | {value}\n")

    fig1 = fig_horario(df)
    fig1.savefig("horario_datajud.jpg")
    plt.close(fig1)

    fig2 = fig_pizza(df)
    fig2.savefig("pizza_expediente_datajud.jpg")
    plt.close(fig2)

    fig3 = fig_mensal(df)
    fig3.savefig("ajuizamentos_mensais_datajud.jpg")
    plt.close(fig3)

    fig4 = fig_fluxo_mensal(df)
    fig4.savefig("fluxo_mensal_datajud.jpg")
    plt.close(fig4)

    fig5 = fig_tempo_tramitacao_boxplot(df)
    fig5.savefig("tempo_tramitacao_boxplot_datajud.jpg")
    plt.close(fig5)

    fig6 = fig_heatmap_dia_hora(df)
    fig6.savefig("heatmap_dia_hora_datajud.jpg")
    plt.close(fig6)


def render() -> None:
    st.set_page_config(page_title="Jurimetria com a API DataJud", layout="wide")
    st.title("Jurimetria com a API DataJud")
    st.markdown(
        "Por **Lucas Martins**  \n"
        "GitHub: [@lucaslmfbib](https://github.com/lucaslmfbib) | "
        "LinkedIn: [lucaslmf](https://www.linkedin.com/in/lucaslmf/) | "
        "Instagram: [@lucaslmf_](https://www.instagram.com/lucaslmf_/)"
    )
    api_key_env = os.getenv("DATAJUD_API_KEY", "").strip()

    with st.sidebar:
        st.header("Configuracao")
        if api_key_env:
            st.caption("DATAJUD_API_KEY detectada. O campo de chave pode ficar vazio.")
        api_key_input = st.text_input(
            "API Key",
            type="password",
            help="Use formato: APIKey ...",
            placeholder="APIKey ...",
        )
        st.markdown(
            "[Onde obter API Key (DataJud Wiki)](https://datajud-wiki.cnj.jus.br/api-publica/acesso/)"
        )
        tribunal_sigla = st.text_input("Tribunal (sigla)", value="tjmg", help="Ex.: tjmg, tjsp, tjrj")
        classe_codigo = st.number_input("Classe codigo", min_value=1, value=12729, step=1)
        st.markdown(
            "[Consultar codigos de classe (CNJ)](https://www.cnj.jus.br/sgt/consulta_publica_classes.php)"
        )
        numero_processo = st.text_input(
            "Numero do processo (opcional)",
            placeholder="Ex.: 50012345620248130024",
            help="Se preenchido, a consulta usa o numero do processo em vez da classe.",
        )
        st.caption("Ao buscar por numero do processo, selecione o tribunal correto.")
        size = st.number_input("Quantidade", min_value=1, max_value=10000, value=2000, step=100)
        auto_url = build_url(tribunal_sigla)
        url = auto_url
        st.caption(f"URL usada: {url}")
        executar = st.button("Executar consulta", use_container_width=True)
        if size > 3000:
            st.warning("Consultas acima de 3000 podem ficar lentas.")

    api_key = api_key_input.strip() or api_key_env

    if executar:
        if not api_key:
            st.error("Informe a API Key no campo ou defina DATAJUD_API_KEY.")
            return

        with st.spinner("Buscando dados no DataJud..."):
            started = time.perf_counter()
            try:
                hits = fetch_hits(
                    api_key=api_key,
                    classe_codigo=int(classe_codigo),
                    size=int(size),
                    url=url,
                    numero_processo=numero_processo,
                )
                df_anpp = hits_to_dataframe(hits)
                top_100 = build_top_100(df_anpp)

                # Se a amostra vier curta para histórico mensal, tenta ampliar só para o gráfico.
                df_mensal = df_anpp
                if not numero_processo.strip() and int(size) < 10000:
                    meses_base = len(monthly_counts(df_anpp, max_meses=12))
                    if meses_base < 12:
                        try:
                            hits_mensal = fetch_hits(
                                api_key=api_key,
                                classe_codigo=int(classe_codigo),
                                size=10000,
                                url=url,
                                numero_processo="",
                            )
                            df_mensal_candidato = hits_to_dataframe(hits_mensal)
                            meses_candidato = len(monthly_counts(df_mensal_candidato, max_meses=12))
                            if meses_candidato > meses_base:
                                df_mensal = df_mensal_candidato
                                st.info(
                                    "Para o gráfico mensal, usei amostra ampliada (10.000 registros)."
                                )
                        except Exception:
                            pass
            except HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 401:
                    st.error(
                        "401 Unauthorized: chave API invalida/expirada ou sem permissao para este endpoint. "
                        "Use no formato 'APIKey ...'."
                    )
                else:
                    st.exception(exc)
                return
            except Exception as exc:
                st.exception(exc)
                return
        elapsed = time.perf_counter() - started

        st.session_state["df_anpp"] = df_anpp
        st.session_state["df_mensal"] = df_mensal
        st.session_state["top_100"] = top_100
        st.session_state["hits"] = hits
        st.success(f"Consulta concluida em {elapsed:.1f}s. Registros: {len(df_anpp)}")

    if "df_anpp" not in st.session_state:
        st.info("Preencha a chave e clique em 'Executar consulta'. Comece com 1000 ou 2000 registros.")
        return

    df_anpp = st.session_state["df_anpp"]
    df_mensal = st.session_state.get("df_mensal", df_anpp)
    top_100 = st.session_state["top_100"]
    df_view = dataframe_for_display(df_anpp)
    total_assuntos = (
        df_anpp["assuntos"].explode().dropna().astype(str).nunique()
        if "assuntos" in df_anpp.columns
        else 0
    )

    st.subheader("Resumo")
    c1, c2, c3 = st.columns(3)
    c1.metric("Registros", f"{len(df_anpp):,}".replace(",", "."))
    c2.metric("Assuntos unicos", str(total_assuntos))
    c3.metric("Orgaos julgadores", str(df_anpp["orgao_julgador"].nunique()))

    st.subheader("Tabela")
    st.caption("Tabela simplificada (colunas pesadas removidas) para evitar travamento.")
    st.dataframe(df_view.head(1000), use_container_width=True, height=350)

    st.subheader("Top 100 por municipio e orgao julgador")
    top_100_df = top_100_to_dataframe(top_100)
    st.dataframe(top_100_df, use_container_width=True, height=350)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Horario")
        st.pyplot(fig_horario(df_anpp), clear_figure=True)
    with col_b:
        st.subheader("Expediente x fora")
        st.pyplot(fig_pizza(df_anpp), clear_figure=True)

    st.subheader("Ajuizamentos mensais")
    st.pyplot(fig_mensal(df_mensal), clear_figure=True)

    st.subheader("Fluxo mensal")
    st.caption("Atualizados usa 'ultima_atualizacao' como proxy de andamento/saida.")
    st.pyplot(fig_fluxo_mensal(df_mensal), clear_figure=True)

    st.subheader("Tempo de tramitacao por orgao")
    st.pyplot(fig_tempo_tramitacao_boxplot(df_anpp), clear_figure=True)

    st.subheader("Heatmap dia x hora")
    st.pyplot(fig_heatmap_dia_hora(df_anpp), clear_figure=True)

    st.subheader("Downloads")
    csv_bytes = df_anpp.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Baixar consulta_datajud.csv",
        data=csv_bytes,
        file_name="consulta_datajud.csv",
        mime="text/csv",
    )

    txt_buffer = io.StringIO()
    for index, value in top_100.items():
        txt_buffer.write(f"{index[0]} | {index[1]} | {value}\n")
    st.download_button(
        "Baixar top_100_datajud.txt",
        data=txt_buffer.getvalue().encode("utf-8"),
        file_name="top_100_datajud.txt",
        mime="text/plain",
    )

    if st.button("Salvar artefatos na pasta do projeto"):
        save_outputs(df_anpp, top_100)
        st.success("Arquivos salvos na pasta do projeto.")


if __name__ == "__main__":
    render()
