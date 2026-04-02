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
CNJ_SIGLAS_URL = "https://www.cnj.jus.br/poder-judiciario/tribunais/"
CNJ_CLASSES_URL = "https://www.cnj.jus.br/sgt/consulta_publica_classes.php"
MAX_PAGE_SIZE = 10000
MAX_TOTAL_SIZE = 50000

CODIGOS_TJM = [
    (11041, "Inquerito Policial Militar"),
    (11030, "Processo Criminal - Militar"),
    (279, "Inquerito Policial"),
    (11955, "Cautelar Inominada Criminal"),
    (325, "Conflito de Jurisdicao"),
    (15423, "Revisao Judicial - Conselho de Justificacao"),
    (120, "Mandado de Seguranca Civel"),
]

CODIGOS_TRT = [
    (985, "Acao Trabalhista - Rito Ordinario"),
    (1125, "Acao Trabalhista - Rito Sumarissimo"),
    (1126, "Acao Trabalhista - Rito Sumario (Alcada)"),
    (980, "Acao de Cumprimento"),
    (986, "Inquerito para Apuracao de Falta Grave"),
    (112, "Homologacao de Transacao Extrajudicial"),
    (987, "Dissidio Coletivo"),
    (988, "Dissidio Coletivo de Greve"),
    (1202, "Reclamacao"),
    (1009, "Recurso Ordinario Trabalhista"),
]

CODIGOS_TJ = [
    (436, "Procedimento do Juizado Especial Civel"),
    (14695, "Procedimento do Juizado Especial da Fazenda Publica"),
    (156, "Cumprimento de Sentenca"),
    (12154, "Execucao de Titulo Extrajudicial"),
    (12079, "Execucao de Titulo Extrajudicial contra a Fazenda Publica"),
    (1116, "Execucao Fiscal"),
    (198, "Apelacao"),
    (202, "Agravo de Instrumento"),
    (1690, "Acao Civil Publica"),
    (64, "Acao Civil de Improbidade Administrativa"),
]

CODIGOS_TRF = [
    (156, "Cumprimento de Sentenca"),
    (12154, "Execucao de Titulo Extrajudicial"),
    (12079, "Execucao de Titulo Extrajudicial contra a Fazenda Publica"),
    (1116, "Execucao Fiscal"),
    (198, "Apelacao"),
    (202, "Agravo de Instrumento"),
    (199, "Reexame Necessario"),
    (283, "Acao Penal - Procedimento Ordinario"),
    (10943, "Acao Penal - Procedimento Sumario"),
    (308, "Medidas Cautelares"),
]

CODIGOS_STJ = [
    (15228, "Queixa-Crime"),
    (11881, "Agravo em Recurso Especial"),
    (1031, "Recurso Ordinario"),
    (1044, "Agravo de Instrumento"),
    (1670, "Acao de Improbidade Administrativa"),
]

CODIGOS_TST = [
    (1008, "Recurso de Revista"),
    (1002, "Agravo de Instrumento em Recurso de Revista"),
    (11882, "Recurso de Revista com Agravo"),
    (1009, "Recurso Ordinario Trabalhista"),
    (1004, "Agravo de Peticao"),
    (987, "Dissidio Coletivo"),
    (988, "Dissidio Coletivo de Greve"),
    (980, "Acao de Cumprimento"),
    (1202, "Reclamacao"),
    (976, "Acao Anulatoria de Clausulas Convencionais"),
]

CODIGOS_TSE = [
    (11525, "Processos Civeis-Eleitorais"),
    (11526, "Acao de Impugnacao de Mandato Eletivo"),
    (11527, "Acao de Investigacao Judicial Eleitoral"),
    (11528, "Acao Penal Eleitoral"),
    (11533, "Recurso Contra Expedicao de Diploma"),
    (11541, "Representacao"),
    (11549, "Recurso Especial Eleitoral"),
    (11550, "Recurso Ordinario"),
]

CODIGOS_CONSELHOS = [
    (11887, "Acompanhamento de Cumprimento de Decisao"),
    (1298, "Processo Administrativo"),
    (1299, "Recurso Administrativo"),
    (1308, "Sindicancia"),
    (11892, "Revisao Disciplinar"),
    (1301, "Reclamacao Disciplinar"),
    (1264, "Processo Administrativo em Face de Magistrado"),
    (1262, "Processo Administrativo em Face de Servidor"),
]


@st.cache_resource(show_spinner=False)
def get_plt() -> Any:
    # Evita custo de import no boot inicial do app.
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mplconfig")
    return importlib.import_module("matplotlib.pyplot")


def resolve_api_key() -> str:
    key_secret = ""
    try:
        key_secret = str(st.secrets.get("DATAJUD_API_KEY", "")).strip()
    except Exception:
        key_secret = ""
    key_env = os.getenv("DATAJUD_API_KEY", "").strip()
    return key_secret or key_env


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


def normalize_tribunal_sigla(raw_sigla: str) -> str:
    return (raw_sigla or "").strip().lower()


def get_codigo_sugestoes(tribunal_sigla: str) -> dict[str, Any]:
    tribunal = normalize_tribunal_sigla(tribunal_sigla)
    if tribunal in {"tjmmg", "tjmrs", "tjmsp"} or tribunal.startswith("tjm"):
        return {
            "categoria": "Tribunal de Justica Militar",
            "titulo": f"Sugestoes para {tribunal or 'tjm'}",
            "codigos": CODIGOS_TJM,
            "observacao": "Base inicial util para TJMMG, TJMRS e TJMSP.",
            "endpoint_publico": True,
        }
    if tribunal.startswith("trt"):
        return {
            "categoria": "Tribunal Regional do Trabalho",
            "titulo": f"Sugestoes para {tribunal or 'trt'}",
            "codigos": CODIGOS_TRT,
            "observacao": "Base inicial util para TRT1 ate TRT24.",
            "endpoint_publico": True,
        }
    if tribunal.startswith("trf"):
        return {
            "categoria": "Tribunal Regional Federal",
            "titulo": f"Sugestoes para {tribunal or 'trf'}",
            "codigos": CODIGOS_TRF,
            "observacao": "Base inicial util para TRF1 ate TRF6.",
            "endpoint_publico": True,
        }
    if tribunal in {"stj"}:
        return {
            "categoria": "Tribunal Superior",
            "titulo": "Sugestoes para STJ",
            "codigos": CODIGOS_STJ,
            "observacao": "Use classes recursais e originarias do STJ.",
            "endpoint_publico": True,
        }
    if tribunal in {"tst"}:
        return {
            "categoria": "Tribunal Superior",
            "titulo": "Sugestoes para TST",
            "codigos": CODIGOS_TST,
            "observacao": "Base inicial util para consultas no TST.",
            "endpoint_publico": True,
        }
    if tribunal in {"tse"}:
        return {
            "categoria": "Tribunal Superior",
            "titulo": "Sugestoes para TSE",
            "codigos": CODIGOS_TSE,
            "observacao": "Base inicial util para processos civeis-eleitorais e recursos.",
            "endpoint_publico": True,
        }
    if tribunal in {"stm"}:
        return {
            "categoria": "Tribunal Superior",
            "titulo": "Sugestoes para STM",
            "codigos": CODIGOS_TJM,
            "observacao": "O STM compartilha a base militar para um teste inicial.",
            "endpoint_publico": True,
        }
    if tribunal in {"cnj", "cjf", "csjt"}:
        return {
            "categoria": "Conselho",
            "titulo": f"Sugestoes para {tribunal.upper()}",
            "codigos": CODIGOS_CONSELHOS,
            "observacao": "Predominam classes administrativas e disciplinares.",
            "endpoint_publico": False,
        }
    if tribunal in {"stf"}:
        return {
            "categoria": "Tribunal Superior",
            "titulo": "Sugestoes para STF",
            "codigos": [],
            "observacao": "O STF nao aparece na lista publica de endpoints do DataJud consultada pelo app.",
            "endpoint_publico": False,
        }
    if tribunal.startswith("tj"):
        return {
            "categoria": "Tribunal de Justica",
            "titulo": f"Sugestoes para {tribunal or 'tj'}",
            "codigos": CODIGOS_TJ,
            "observacao": "Base inicial util para TJMG, TJSP, TJRJ e demais TJs.",
            "endpoint_publico": True,
        }
    return {
        "categoria": "Nao mapeado",
        "titulo": "Sugestoes basicas",
        "codigos": [],
        "observacao": "Consulte as siglas e os codigos oficiais do CNJ para este tribunal.",
        "endpoint_publico": True,
    }


def render_codigo_sugestoes(tribunal_sigla: str) -> None:
    sugestoes = get_codigo_sugestoes(tribunal_sigla)
    with st.expander("Sugestoes de codigos para este tribunal", expanded=True):
        st.caption(f"Categoria detectada: {sugestoes['categoria']}")
        if sugestoes["codigos"]:
            linhas = [
                f"- `{codigo}` - {classe}" for codigo, classe in sugestoes["codigos"]
            ]
            st.markdown("\n".join(linhas))
        else:
            st.caption("Nenhuma sugestao automatica disponivel para esta sigla.")
        st.caption(str(sugestoes["observacao"]))
        if not bool(sugestoes["endpoint_publico"]):
            st.warning(
                "A lista publica de endpoints do DataJud nao mostra endpoint publico para esta sigla. "
                "A consulta pode nao funcionar no app."
            )


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
    incluir_movimentos: bool = False,
    modo_consulta: str = "classe_ou_processo",
) -> list[dict[str, Any]]:
    numero_limpo = normalize_numero_processo(numero_processo)
    if numero_limpo:
        query: dict[str, Any] = {"match": {"numeroProcesso": numero_limpo}}
    elif modo_consulta == "mapa_tribunal":
        query = {"match_all": {}}
    else:
        query = {"match": {"classe.codigo": classe_codigo}}

    campos_source = [
        "numeroProcesso",
        "classe.codigo",
        "classe.nome",
        "dataAjuizamento",
        "dataHoraUltimaAtualizacao",
        "formato",
        "orgaoJulgador.nome",
        "orgaoJulgador.codigoMunicipioIBGE",
        "grau",
        "assuntos",
    ]
    if incluir_movimentos:
        campos_source.append("movimentos")

    payload = {
        "size": size,
        "_source": campos_source,
        "query": query,
        "sort": [{"dataAjuizamento": {"order": "desc"}}],
    }
    headers = {
        "Authorization": normalize_api_key(api_key),
        "Content-Type": "application/json",
    }

    if size <= MAX_PAGE_SIZE:
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()
        return data.get("hits", {}).get("hits", [])

    # Para consultas acima de 10.000 registros, usa paginação oficial com search_after.
    all_hits: list[dict[str, Any]] = []
    search_after: Any = None
    sort = [{"id.keyword": {"order": "asc"}}]

    while len(all_hits) < size:
        page_size = min(MAX_PAGE_SIZE, size - len(all_hits))
        paged_payload = {
            "size": page_size,
            "_source": campos_source,
            "query": query,
            "sort": sort,
        }
        if search_after is not None:
            paged_payload["search_after"] = search_after

        response = requests.post(url, headers=headers, json=paged_payload, timeout=120)
        response.raise_for_status()
        data = response.json()
        page_hits = data.get("hits", {}).get("hits", [])
        if not page_hits:
            break

        all_hits.extend(page_hits)
        search_after = page_hits[-1].get("sort")
        if not search_after or len(page_hits) < page_size:
            break

    return all_hits


@st.cache_data(show_spinner=False, ttl=1200)
def hits_to_dataframe(hits: list[dict[str, Any]], processar_movimentos: bool = False) -> pd.DataFrame:
    rows: list[list[Any]] = []

    for hit in hits:
        source = hit.get("_source", {}) if isinstance(hit, dict) else {}
        classe = source.get("classe", {}) if isinstance(source.get("classe"), dict) else {}
        orgao = (
            source.get("orgaoJulgador", {})
            if isinstance(source.get("orgaoJulgador"), dict)
            else {}
        )

        movimentos_raw = source.get("movimentos", [])
        if processar_movimentos:
            movimentos_valor = movimentos_raw if isinstance(movimentos_raw, list) else []
        else:
            movimentos_valor = len(movimentos_raw) if isinstance(movimentos_raw, list) else 0

        rows.append(
            [
                source.get("numeroProcesso"),
                classe.get("codigo"),
                classe.get("nome"),
                source.get("dataAjuizamento"),
                source.get("dataHoraUltimaAtualizacao"),
                source.get("formato"),
                orgao.get("nome"),
                orgao.get("codigoMunicipioIBGE"),
                source.get("grau"),
                source.get("assuntos", []),
                movimentos_valor,
                hit.get("sort") if isinstance(hit, dict) else None,
            ]
        )

    columns = [
        "numero_processo",
        "classe_codigo",
        "classe",
        "data_ajuizamento",
        "ultima_atualizacao",
        "formato",
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
    if processar_movimentos:
        df["movimentos"] = df["movimentos"].apply(parse_movimentos)
        df["movimentos"] = df["movimentos"].apply(
            lambda x: sorted(x, key=lambda tup: tup[2] if len(tup) > 2 else pd.NaT, reverse=True)
        )
    else:
        df["movimentos"] = pd.to_numeric(df["movimentos"], errors="coerce").fillna(0).astype(int)
    df["data_ajuizamento"] = df["data_ajuizamento"].apply(to_sao_paulo_datetime)
    df["ultima_atualizacao"] = df["ultima_atualizacao"].apply(to_sao_paulo_datetime)
    df = df.sort_values(
        by=["data_ajuizamento", "numero_processo"],
        ascending=[False, True],
        na_position="last",
    ).reset_index(drop=True)
    return df


def build_top_100(df_anpp: pd.DataFrame) -> pd.Series:
    if df_anpp.empty:
        return pd.Series(dtype="int64")
    return (
        df_anpp.groupby(["municipio", "orgao_julgador"])
        .size()
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


def top_codigos_dataframe(df_anpp: pd.DataFrame, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "classe_codigo" not in df_anpp.columns:
        return pd.DataFrame(columns=["classe_codigo", "classe", "quantidade"])

    base = df_anpp[["classe_codigo", "classe"]].copy()
    base["classe_codigo"] = pd.to_numeric(base["classe_codigo"], errors="coerce").astype("Int64")
    base["classe"] = base["classe"].fillna("").astype(str).str.strip()
    base = base[base["classe_codigo"].notna()]
    if base.empty:
        return pd.DataFrame(columns=["classe_codigo", "classe", "quantidade"])

    referencias = (
        base[base["classe"] != ""]
        .drop_duplicates(subset=["classe_codigo"])
        .rename(columns={"classe": "classe_referencia"})
    )
    resultado = (
        base.groupby("classe_codigo", dropna=False)
        .size()
        .reset_index(name="quantidade")
        .sort_values("quantidade", ascending=False)
        .head(max_items)
        .merge(referencias[["classe_codigo", "classe_referencia"]], on="classe_codigo", how="left")
        .rename(columns={"classe_referencia": "classe"})
    )
    resultado["classe_codigo"] = resultado["classe_codigo"].astype("Int64").astype(str)
    return resultado[["classe_codigo", "classe", "quantidade"]]


def top_classes_dataframe(df_anpp: pd.DataFrame, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "classe" not in df_anpp.columns:
        return pd.DataFrame(columns=["classe", "quantidade"])

    classes = df_anpp["classe"].fillna("").astype(str).str.strip()
    classes = classes[classes != ""]
    if classes.empty:
        return pd.DataFrame(columns=["classe", "quantidade"])

    return classes.value_counts().head(max_items).rename_axis("classe").reset_index(name="quantidade")


def top_assuntos_dataframe(df_anpp: pd.DataFrame, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "assuntos" not in df_anpp.columns:
        return pd.DataFrame(columns=["assunto", "quantidade"])

    assuntos = df_anpp["assuntos"].explode().dropna().astype(str).str.strip()
    assuntos = assuntos[assuntos != ""]
    if assuntos.empty:
        return pd.DataFrame(columns=["assunto", "quantidade"])

    return assuntos.value_counts().head(max_items).rename_axis("assunto").reset_index(name="quantidade")


def dataframe_for_display(df_anpp: pd.DataFrame, max_rows: int = 400) -> pd.DataFrame:
    if df_anpp.empty:
        return df_anpp

    def movimentos_count(value: Any) -> int:
        if isinstance(value, list):
            return len(value)
        try:
            return int(value)
        except Exception:
            return 0

    df_view = df_anpp.head(max_rows).copy()
    df_view["assuntos"] = df_view["assuntos"].apply(
        lambda x: ", ".join(x[:3]) + (" ..." if len(x) > 3 else "")
        if isinstance(x, list)
        else ""
    )
    df_view["qtd_movimentos"] = df_view["movimentos"].apply(movimentos_count)
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
    api_key = resolve_api_key()

    with st.sidebar:
        st.header("Configuracao")
        if api_key:
            st.success("API Key configurada no servidor.")
        else:
            st.error("API Key nao configurada no servidor.")
            st.caption("Configure DATAJUD_API_KEY em Streamlit Secrets (ou variavel de ambiente local).")
        st.markdown(
            "[Onde obter API Key (DataJud Wiki)](https://datajud-wiki.cnj.jus.br/api-publica/acesso/)"
        )
        tribunal_sigla = st.text_input(
            "Tribunal (sigla)",
            value="tjmg",
            help="Ex.: tjmg, tjmmg, trf1, trt3, stj, tst, tse, stm.",
        )
        st.markdown(f"[Consultar siglas de tribunais (CNJ)]({CNJ_SIGLAS_URL})")
        modo_consulta_label = st.radio(
            "Modo de consulta",
            ("Classe ou processo", "Mapa do tribunal"),
            help="O mapa do tribunal ignora o filtro por classe e mostra os codigos, classes e assuntos mais comuns na amostra da sigla.",
        )
        modo_consulta = "mapa_tribunal" if modo_consulta_label == "Mapa do tribunal" else "classe_ou_processo"
        classe_codigo = st.number_input(
            "Classe codigo",
            min_value=1,
            value=12729,
            step=1,
            disabled=modo_consulta == "mapa_tribunal",
        )
        render_codigo_sugestoes(tribunal_sigla)
        st.markdown(
            f"[Consultar codigos de classe (CNJ)]({CNJ_CLASSES_URL})"
        )
        numero_processo = st.text_input(
            "Numero do processo (opcional)",
            placeholder="Ex.: 50012345620248130024",
            help="Se preenchido, a consulta usa o numero do processo em vez da classe.",
            disabled=modo_consulta == "mapa_tribunal",
        )
        if modo_consulta == "mapa_tribunal":
            st.caption(
                "No mapa do tribunal, o app usa a sigla para buscar uma amostra recente e montar rankings de codigos, classes e assuntos."
            )
        else:
            st.caption("Ao buscar por numero do processo, selecione o tribunal correto.")
        modo_rapido = st.checkbox(
            "Modo rapido (recomendado)",
            value=True,
            help="Reduz processamento interno para acelerar a resposta.",
        )
        ampliar_historico = st.checkbox(
            "Ampliar historico mensal automaticamente (mais lento)",
            value=False,
            help="Faz nova consulta com 10.000 registros para tentar preencher 12 meses no grafico mensal.",
        )
        mostrar_graficos_avancados = st.checkbox(
            "Exibir graficos avancados (mais lento)",
            value=False,
            help="Ative para ver fluxo mensal, tempo de tramitacao e heatmap.",
        )
        size = st.number_input("Quantidade", min_value=1, max_value=MAX_TOTAL_SIZE, value=700, step=100)
        if size > MAX_PAGE_SIZE:
            st.info(
                "Acima de 10.000 registros, o app pagina automaticamente a consulta no DataJud. "
                "Isso pode deixar a resposta mais lenta."
            )
        auto_url = build_url(tribunal_sigla)
        url = auto_url
        st.caption(f"URL usada: {url}")
        executar = st.button("Executar consulta", use_container_width=True)
        if size > 2000:
            st.warning("Consultas acima de 2000 podem ficar lentas.")

    if executar:
        if not api_key:
            st.error("API Key ausente. Configure DATAJUD_API_KEY no servidor.")
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
                    incluir_movimentos=not modo_rapido,
                    modo_consulta=modo_consulta,
                )
                df_anpp = hits_to_dataframe(hits, processar_movimentos=not modo_rapido)
                top_100 = build_top_100(df_anpp)
                top_codigos = top_codigos_dataframe(df_anpp) if modo_consulta == "mapa_tribunal" else pd.DataFrame()
                top_classes = top_classes_dataframe(df_anpp) if modo_consulta == "mapa_tribunal" else pd.DataFrame()
                top_assuntos = top_assuntos_dataframe(df_anpp) if modo_consulta == "mapa_tribunal" else pd.DataFrame()

                # Se a amostra vier curta para histórico mensal, tenta ampliar só para o gráfico.
                df_mensal = df_anpp
                usar_numero_processo = (
                    modo_consulta != "mapa_tribunal" and bool(normalize_numero_processo(numero_processo))
                )
                if ampliar_historico and not usar_numero_processo and int(size) < 10000:
                    meses_base = len(monthly_counts(df_anpp, max_meses=12))
                    if meses_base < 12:
                        try:
                            hits_mensal = fetch_hits(
                                api_key=api_key,
                                classe_codigo=int(classe_codigo),
                                size=10000,
                                url=url,
                                numero_processo="",
                                incluir_movimentos=False,
                                modo_consulta=modo_consulta,
                            )
                            df_mensal_candidato = hits_to_dataframe(hits_mensal, processar_movimentos=False)
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
        st.session_state["modo_consulta"] = modo_consulta
        st.session_state["top_codigos"] = top_codigos
        st.session_state["top_classes"] = top_classes
        st.session_state["top_assuntos"] = top_assuntos
        st.success(f"Consulta concluida em {elapsed:.1f}s. Registros: {len(df_anpp)}")

    if "df_anpp" not in st.session_state:
        st.info("Preencha a chave e clique em 'Executar consulta'. Comece com 1000 ou 2000 registros.")
        return

    df_anpp = st.session_state["df_anpp"]
    df_mensal = st.session_state.get("df_mensal", df_anpp)
    top_100 = st.session_state["top_100"]
    modo_consulta = st.session_state.get("modo_consulta", "classe_ou_processo")
    top_codigos = st.session_state.get("top_codigos", pd.DataFrame())
    top_classes = st.session_state.get("top_classes", pd.DataFrame())
    top_assuntos = st.session_state.get("top_assuntos", pd.DataFrame())
    df_view = dataframe_for_display(df_anpp, max_rows=400)
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
    st.caption("Tabela simplificada (amostra de ate 400 linhas) para evitar travamento.")
    st.dataframe(df_view, use_container_width=True, height=350)

    st.subheader("Top 100 por municipio e orgao julgador")
    top_100_df = top_100_to_dataframe(top_100)
    st.dataframe(top_100_df, use_container_width=True, height=350)

    if modo_consulta == "mapa_tribunal":
        st.subheader("Mapa do tribunal na amostra atual")
        st.caption("Os rankings abaixo usam apenas os registros retornados nesta consulta para a sigla selecionada.")
        col_codigos, col_classes, col_assuntos = st.columns(3)
        with col_codigos:
            st.markdown("**Top 10 codigos**")
            st.dataframe(top_codigos, use_container_width=True, height=320)
        with col_classes:
            st.markdown("**Top 10 classes**")
            st.dataframe(top_classes, use_container_width=True, height=320)
        with col_assuntos:
            st.markdown("**Top 10 assuntos**")
            st.dataframe(top_assuntos, use_container_width=True, height=320)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Horario")
        st.pyplot(fig_horario(df_anpp), clear_figure=True)
    with col_b:
        st.subheader("Expediente x fora")
        st.pyplot(fig_pizza(df_anpp), clear_figure=True)

    st.subheader("Ajuizamentos mensais")
    st.pyplot(fig_mensal(df_mensal), clear_figure=True)

    if mostrar_graficos_avancados:
        st.subheader("Fluxo mensal")
        st.caption("Atualizados usa 'ultima_atualizacao' como proxy de andamento/saida.")
        st.pyplot(fig_fluxo_mensal(df_mensal), clear_figure=True)

        st.subheader("Tempo de tramitacao por orgao")
        st.pyplot(fig_tempo_tramitacao_boxplot(df_anpp), clear_figure=True)

        st.subheader("Heatmap dia x hora")
        st.pyplot(fig_heatmap_dia_hora(df_anpp), clear_figure=True)
    else:
        st.caption("Graficos avancados ocultos para resposta mais rapida. Ative na barra lateral.")

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
