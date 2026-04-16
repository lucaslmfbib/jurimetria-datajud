from datetime import date, datetime, time as dt_time
import html
import io
import importlib
import os
import time
from typing import Any
import unicodedata

import pandas as pd
import requests
from requests import HTTPError, RequestException, Timeout
import streamlit as st

URL_PADRAO = "https://api-publica.datajud.cnj.jus.br/api_publica_tjmg/_search"
URL_TEMPLATE = "https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search"
CNJ_SIGLAS_URL = "https://www.cnj.jus.br/poder-judiciario/tribunais/"
CNJ_CLASSES_URL = "https://www.cnj.jus.br/sgt/consulta_publica_classes.php"
MAX_PAGE_SIZE = 10000
MAX_TOTAL_SIZE = 50000
DATAJUD_TIMEOUT_SECONDS = 120
DATAJUD_RETRYABLE_STATUS = {502, 503, 504}
DATAJUD_MAX_RETRIES = 2

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

DECISAO_MOVIMENTO_HINTS = (
    "procedent",
    "improcedent",
    "sentenc",
    "acordao",
    "julgado",
    "julgamento",
    "homolog",
    "extint",
    "arquiv",
    "liminar",
    "tutela",
    "conden",
    "absolv",
    "provimento",
    "improvido",
    "prejudicad",
    "nao conhecid",
    "ordem",
    "seguranca",
    "punibilidade",
    "pronunc",
    "despronunc",
    "impronunc",
    "receb",
    "denuncia",
    "queixa",
    "acordo",
    "concili",
)

MOVIMENTO_NAO_DECISORIO_HINTS = (
    "conclus",
    "juntad",
    "certidao",
    "expedid",
    "expedicao",
    "remetid",
    "remessa",
    "intimacao",
    "publicacao",
    "publicado",
    "vista",
    "decurso",
    "recebimento em secretaria",
    "recebidos os autos",
    "redistribu",
    "baixa",
    "andamento",
)

CATEGORIA_NAO_CLASSIFICADA = "Decisao identificada, mas nao classificada"
POLARIDADE_FAVORAVEL = "Favoravel estimado"
POLARIDADE_DESFAVORAVEL = "Desfavoravel estimado"
POLARIDADE_MISTA = "Misto/Parcial"
POLARIDADE_NEUTRA = "Neutro/Processual"
POLARIDADE_INDEFINIDA = "Indefinido"

POLARIDADE_DESFECHO_MAP = {
    "Procedente": POLARIDADE_FAVORAVEL,
    "Parcialmente procedente": POLARIDADE_MISTA,
    "Recurso provido": POLARIDADE_FAVORAVEL,
    "Parcial provimento": POLARIDADE_MISTA,
    "Recurso improvido": POLARIDADE_DESFAVORAVEL,
    "Improcedente": POLARIDADE_DESFAVORAVEL,
    "Homologacao de acordo": POLARIDADE_FAVORAVEL,
    "Homologacao": POLARIDADE_NEUTRA,
    "Ordem/Seguranca concedida": POLARIDADE_FAVORAVEL,
    "Ordem/Seguranca denegada": POLARIDADE_DESFAVORAVEL,
    "Absolvicao sumaria": POLARIDADE_FAVORAVEL,
    "Absolvicao": POLARIDADE_FAVORAVEL,
    "Condenacao": POLARIDADE_DESFAVORAVEL,
    "Extincao da punibilidade": POLARIDADE_FAVORAVEL,
    "Extincao/Arquivamento": POLARIDADE_NEUTRA,
    "Recebimento da denuncia/queixa": POLARIDADE_DESFAVORAVEL,
    "Rejeicao da denuncia/queixa": POLARIDADE_FAVORAVEL,
    "Despronuncia": POLARIDADE_FAVORAVEL,
    "Impronuncia": POLARIDADE_FAVORAVEL,
    "Pronuncia": POLARIDADE_DESFAVORAVEL,
    "Tutela/Liminar concedida": POLARIDADE_FAVORAVEL,
    "Tutela/Liminar negada": POLARIDADE_DESFAVORAVEL,
    "Rejeicao/Negativa": POLARIDADE_DESFAVORAVEL,
    "Nao acolhimento": POLARIDADE_DESFAVORAVEL,
    "Acolhimento": POLARIDADE_FAVORAVEL,
    "Indeferimento": POLARIDADE_DESFAVORAVEL,
    "Recebimento/Admissao": POLARIDADE_NEUTRA,
    "Nao conhecido/Prejudicado": POLARIDADE_NEUTRA,
    "ANPP homologado/admitido": POLARIDADE_FAVORAVEL,
    "ANPP nao homologado/rejeitado": POLARIDADE_DESFAVORAVEL,
    CATEGORIA_NAO_CLASSIFICADA: POLARIDADE_INDEFINIDA,
}


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
    if tribunal in {"tjmmg", "tjmrs", "tjmsp"}:
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
    sigla_atual = normalize_tribunal_sigla(tribunal_sigla)
    sigla_mapeada = normalize_tribunal_sigla(str(st.session_state.get("sigla_mapa", "")))
    top_codigos = st.session_state.get("top_codigos", pd.DataFrame())
    qtd_mapa = int(st.session_state.get("qtd_mapa", 0) or 0)
    with st.expander("Sugestoes de codigos para este tribunal", expanded=True):
        if sigla_atual:
            st.caption(f"Sigla atual: {sigla_atual.upper()} | Categoria detectada: {sugestoes['categoria']}")
        else:
            st.caption(f"Categoria detectada: {sugestoes['categoria']}")
        if (
            sigla_atual
            and sigla_atual == sigla_mapeada
            and isinstance(top_codigos, pd.DataFrame)
            and not top_codigos.empty
        ):
            st.markdown("**Codigos mais comuns na amostra da sigla**")
            linhas = [
                f"- `{row['classe_codigo']}` - {row['classe']}"
                for _, row in top_codigos.head(10).iterrows()
            ]
            st.markdown("\n".join(linhas))
            if qtd_mapa:
                st.caption(f"Mapa automatico baseado em ate {qtd_mapa:,} registros recentes da sigla.".replace(",", "."))
        elif sugestoes["codigos"]:
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


def get_estrutura_options(tribunal_sigla: str) -> dict[str, Any]:
    tribunal = normalize_tribunal_sigla(tribunal_sigla)
    opcoes_base = ["Todos"]

    if tribunal in {"tjmmg", "tjmrs", "tjmsp"}:
        return {
            "opcoes": opcoes_base + ["1o Grau", "TJM"],
            "observacao": "O app usa o campo de grau e, quando preciso, o nome do orgao julgador para diferenciar 1o grau e tribunal militar.",
        }
    if tribunal.startswith("tj"):
        return {
            "opcoes": opcoes_base
            + [
                "1o Grau",
                "2o Grau",
                "Juizado Especial",
                "Turmas Recursais",
                "Juizado Especial da Fazenda Publica",
                "Turma Estadual de Uniformizacao",
            ],
            "observacao": "Juizados, turmas recursais e uniformizacao sao estimados principalmente pelo nome do orgao julgador.",
        }
    if tribunal.startswith("trf"):
        return {
            "opcoes": opcoes_base
            + [
                "1o Grau",
                "2o Grau",
                "Juizado Especial",
                "Turmas Recursais",
                "Turma Regional de Uniformizacao",
            ],
            "observacao": "Na Justica Federal, o app combina grau e nome do orgao para sugerir a estrutura.",
        }
    if tribunal == "cjf":
        return {
            "opcoes": opcoes_base + ["CJF", "Turma Nacional de Uniformizacao (CJF)"],
            "observacao": "A TNU e estimada pelo nome do orgao julgador quando aparecer na amostra.",
        }
    if tribunal.startswith("trt"):
        return {
            "opcoes": opcoes_base + ["1o Grau", "2o Grau"],
            "observacao": "Na Justica do Trabalho, o filtro estrutural usa o campo de grau e o nome do orgao julgador.",
        }
    if tribunal == "tst":
        return {
            "opcoes": opcoes_base + ["TST"],
            "observacao": "A propria sigla ja identifica o tribunal superior trabalhista.",
        }
    if tribunal == "csjt":
        return {
            "opcoes": opcoes_base + ["CSJT"],
            "observacao": "A propria sigla ja identifica o conselho superior trabalhista.",
        }
    if tribunal == "stm":
        return {
            "opcoes": opcoes_base + ["1o Grau", "STM"],
            "observacao": "O app diferencia 1o grau e STM pelo grau processual e pelo nome do orgao, quando disponiveis.",
        }
    if tribunal.startswith("tre"):
        return {
            "opcoes": opcoes_base + ["Zonas Eleitorais", "TRE"],
            "observacao": "Na Justica Eleitoral regional, a leitura estrutural usa o nome do orgao julgador e o grau.",
        }
    if tribunal == "tse":
        return {
            "opcoes": opcoes_base + ["TSE"],
            "observacao": "A propria sigla ja identifica o tribunal superior eleitoral.",
        }
    if tribunal in {"stf", "stj", "cnj"}:
        return {
            "opcoes": opcoes_base + [tribunal.upper()],
            "observacao": "A propria sigla ja identifica a estrutura principal deste orgao.",
        }
    return {
        "opcoes": ["Todos"],
        "observacao": "Sem filtro estrutural especifico para esta sigla.",
    }


def format_estrutura_option(estrutura: str) -> str:
    labels = {
        "Todos": "Todos os niveis",
        "1o Grau": "1o grau (varas / juizos)",
        "2o Grau": "2o grau (camaras / turmas)",
        "Juizado Especial": "Juizado Especial",
        "Turmas Recursais": "Turmas Recursais",
        "Juizado Especial da Fazenda Publica": "Juizado da Fazenda Publica",
        "Turma Estadual de Uniformizacao": "Turma Estadual de Uniformizacao",
        "Turma Regional de Uniformizacao": "Turma Regional de Uniformizacao",
        "Turma Nacional de Uniformizacao (CJF)": "TNU (CJF)",
        "Zonas Eleitorais": "Zonas Eleitorais (1o grau eleitoral)",
        "TRE": "TRE (tribunal regional eleitoral)",
        "TJM": "TJM (tribunal militar estadual)",
        "STM": "STM (tribunal militar da Uniao)",
        "TST": "TST",
        "TSE": "TSE",
        "STJ": "STJ",
        "STF": "STF",
        "CNJ": "CNJ",
        "CJF": "CJF",
        "CSJT": "CSJT",
    }
    return labels.get(estrutura, estrutura)


def describe_estrutura_option(estrutura: str) -> str:
    descricoes = {
        "Todos": "Analisa toda a estrutura da sigla selecionada, sem separar por instancia ou orgao especial.",
        "1o Grau": "Em geral, inclui varas, juizos, auditorias, comarcas e unidades de entrada do processo.",
        "2o Grau": "Em geral, inclui camaras, turmas, secoes e o proprio tribunal em grau recursal.",
        "Juizado Especial": "Foca nos juizados especiais identificados no nome do orgao julgador.",
        "Turmas Recursais": "Foca nas turmas recursais ou colegios recursais ligados aos juizados.",
        "Juizado Especial da Fazenda Publica": "Foca nos juizados especiais da Fazenda Publica quando o orgao for identificado assim.",
        "Turma Estadual de Uniformizacao": "Foca na turma estadual que uniformiza entendimento dos juizados.",
        "Turma Regional de Uniformizacao": "Foca na turma regional de uniformizacao da Justica Federal.",
        "Turma Nacional de Uniformizacao (CJF)": "Foca na TNU quando ela aparecer identificada na amostra.",
        "Zonas Eleitorais": "Foca no primeiro grau da Justica Eleitoral, normalmente as zonas eleitorais.",
        "TRE": "Foca no tribunal regional eleitoral.",
        "TJM": "Foca no tribunal de Justica Militar estadual.",
        "STM": "Foca no Superior Tribunal Militar.",
        "TST": "Foca no Tribunal Superior do Trabalho.",
        "TSE": "Foca no Tribunal Superior Eleitoral.",
        "STJ": "Foca no Superior Tribunal de Justica.",
        "STF": "Foca no Supremo Tribunal Federal.",
        "CNJ": "Foca no Conselho Nacional de Justica.",
        "CJF": "Foca no Conselho da Justica Federal.",
        "CSJT": "Foca no Conselho Superior da Justica do Trabalho.",
    }
    return descricoes.get(
        estrutura,
        "Filtro estrutural baseado no grau processual e, quando necessario, no nome do orgao julgador.",
    )


def infer_grau_bucket(grau: Any) -> str:
    text = normalize_search_text(grau)
    if text in {"1", "g1", "1 grau", "1o grau", "primeiro grau"}:
        return "1o Grau"
    if text in {"2", "g2", "2 grau", "2o grau", "segundo grau"}:
        return "2o Grau"
    return ""


def infer_estrutura_label(tribunal_sigla: str, grau: Any, orgao_julgador: Any) -> str:
    tribunal = normalize_tribunal_sigla(tribunal_sigla)
    orgao = normalize_search_text(orgao_julgador)
    grau_bucket = infer_grau_bucket(grau)

    if tribunal in {"stf", "stj", "cnj", "tst", "csjt", "tse"}:
        return tribunal.upper()

    if tribunal == "cjf":
        if "turma nacional de uniformizacao" in orgao or "tnu" in orgao:
            return "Turma Nacional de Uniformizacao (CJF)"
        return "CJF"

    if tribunal in {"tjmmg", "tjmrs", "tjmsp"}:
        if grau_bucket:
            return "TJM" if grau_bucket == "2o Grau" else grau_bucket
        if any(chave in orgao for chave in ("tribunal de justica militar", "tjm", "tribunal pleno")):
            return "TJM"
        if any(chave in orgao for chave in ("auditoria", "conselho de justica", "juizo militar")):
            return "1o Grau"
        return "Nao identificado"

    if tribunal == "stm":
        if grau_bucket:
            return "STM" if grau_bucket == "2o Grau" else grau_bucket
        if "superior tribunal militar" in orgao or orgao == "stm":
            return "STM"
        if "auditoria" in orgao:
            return "1o Grau"
        return "Nao identificado"

    if tribunal.startswith("trt"):
        if grau_bucket:
            return grau_bucket
        if any(chave in orgao for chave in ("tribunal regional do trabalho", "gabinete", "secao especializada")):
            return "2o Grau"
        if any(chave in orgao for chave in ("vara do trabalho", "posto avancado")):
            return "1o Grau"
        return "Nao identificado"

    if tribunal.startswith("trf"):
        if "turma regional de uniformizacao" in orgao or "tru" in orgao:
            return "Turma Regional de Uniformizacao"
        if "turma recursal" in orgao:
            return "Turmas Recursais"
        if "juizado especial" in orgao:
            return "Juizado Especial"
        if grau_bucket:
            return grau_bucket
        if any(chave in orgao for chave in ("tribunal regional federal", "corte especial", "secao", "turma")):
            return "2o Grau"
        if any(chave in orgao for chave in ("vara federal", "subsecao judiciaria")):
            return "1o Grau"
        return "Nao identificado"

    if tribunal.startswith("tre"):
        if "zona eleitoral" in orgao or grau_bucket == "1o Grau":
            return "Zonas Eleitorais"
        return "TRE"

    if tribunal.startswith("tj"):
        if "juizado especial da fazenda publica" in orgao:
            return "Juizado Especial da Fazenda Publica"
        if "turma estadual de uniformizacao" in orgao:
            return "Turma Estadual de Uniformizacao"
        if any(chave in orgao for chave in ("turma recursal", "colegio recursal")):
            return "Turmas Recursais"
        if "juizado especial" in orgao:
            return "Juizado Especial"
        if grau_bucket:
            return grau_bucket
        if any(chave in orgao for chave in ("camara", "orgao especial", "tribunal pleno", "secao civel", "secao criminal")):
            return "2o Grau"
        if any(chave in orgao for chave in ("vara", "foro", "comarca", "juizo")):
            return "1o Grau"
        return "Nao identificado"

    return "Nao identificado"


def add_estrutura_column(df_anpp: pd.DataFrame, tribunal_sigla: str) -> pd.DataFrame:
    if df_anpp.empty:
        return df_anpp.copy()

    df_out = df_anpp.copy()
    graus = df_out["grau"].tolist() if "grau" in df_out.columns else [None] * len(df_out)
    orgaos = (
        df_out["orgao_julgador"].tolist()
        if "orgao_julgador" in df_out.columns
        else [None] * len(df_out)
    )
    df_out["estrutura_tribunal"] = [
        infer_estrutura_label(tribunal_sigla, grau, orgao)
        for grau, orgao in zip(graus, orgaos)
    ]
    return df_out


def filter_dataframe_by_estrutura(
    df_anpp: pd.DataFrame,
    tribunal_sigla: str,
    estrutura_filtro: str,
) -> pd.DataFrame:
    df_out = add_estrutura_column(df_anpp, tribunal_sigla)
    if df_out.empty or not estrutura_filtro or estrutura_filtro == "Todos":
        return df_out
    return df_out.loc[df_out["estrutura_tribunal"] == estrutura_filtro].copy()


def normalize_numero_processo(raw_numero: str) -> str:
    numero = (raw_numero or "").strip()
    somente_digitos = "".join(ch for ch in numero if ch.isdigit())
    return somente_digitos or numero


def coerce_date_value(value: Any) -> Any:
    if value in (None, "", ()):
        return None
    if isinstance(value, tuple):
        if not value:
            return None
        value = value[0]
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def build_data_ajuizamento_range(
    data_inicio: Any = None,
    data_fim: Any = None,
) -> dict[str, Any]:
    inicio = coerce_date_value(data_inicio)
    fim = coerce_date_value(data_fim)
    if not inicio and not fim:
        return {}

    faixa: dict[str, Any] = {}
    if inicio:
        faixa["gte"] = datetime.combine(inicio, dt_time.min).isoformat()
    if fim:
        faixa["lte"] = datetime.combine(fim, dt_time.max).isoformat()
    return {"range": {"dataAjuizamento": faixa}}


def format_periodo_aplicado(data_inicio: Any = None, data_fim: Any = None) -> str:
    inicio = coerce_date_value(data_inicio)
    fim = coerce_date_value(data_fim)
    if inicio and fim:
        return f"{inicio.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')}"
    if inicio:
        return f"a partir de {inicio.strftime('%d/%m/%Y')}"
    if fim:
        return f"ate {fim.strftime('%d/%m/%Y')}"
    return ""


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


def normalize_search_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")


def is_decisive_movement_name(nome: str) -> bool:
    text = normalize_search_text(nome)
    if not text:
        return False
    if any(hint in text for hint in MOVIMENTO_NAO_DECISORIO_HINTS):
        return False
    return any(hint in text for hint in DECISAO_MOVIMENTO_HINTS)


def classify_decision_outcome(nome: str) -> str:
    text = normalize_search_text(nome)
    if not text:
        return ""

    if any(
        trecho in text
        for trecho in (
            "acordo de nao persecu",
            "anpp",
            "nao persecucao penal",
        )
    ):
        if any(
            trecho in text
            for trecho in (
                "nao homolog",
                "não homolog",
                "rejeit",
                "recus",
                "inadmit",
                "nao admit",
                "não admit",
                "indefer",
            )
        ):
            return "ANPP nao homologado/rejeitado"
        if any(
            trecho in text
            for trecho in (
                "homolog",
                "receb",
                "admit",
                "defer",
                "aprov",
                "celebr",
                "ratific",
            )
        ):
            return "ANPP homologado/admitido"

    if any(
        trecho in text
        for trecho in (
            "parcialmente procedente",
            "procedente em parte",
            "parcial procedente",
        )
    ):
        return "Parcialmente procedente"
    if any(
        trecho in text
        for trecho in (
            "parcial provimento",
            "parcialmente provido",
            "provido em parte",
            "conhecido e provido em parte",
        )
    ):
        return "Parcial provimento"
    if any(
        trecho in text
        for trecho in (
            "conhecido e provido",
            "recurso conhecido e provido",
        )
    ):
        return "Recurso provido"
    if any(
        trecho in text
        for trecho in (
            "conhecido e improvido",
            "conhecido e nao provido",
            "recurso conhecido e improvido",
        )
    ):
        return "Recurso improvido"
    if any(trecho in text for trecho in ("improcedente", "improcedencia")):
        return "Improcedente"
    if any(
        trecho in text
        for trecho in (
            "negar provimento",
            "negado provimento",
            "improvido",
            "recurso nao provido",
        )
    ):
        return "Recurso improvido"
    if any(
        trecho in text
        for trecho in (
            "dar provimento",
            "deu provimento",
            "recurso provido",
            "provido",
        )
    ):
        return "Recurso provido"
    if any(trecho in text for trecho in ("procedente", "procedencia")):
        return "Procedente"
    if "homolog" in text and any(
        trecho in text for trecho in ("acordo", "transacao", "conciliacao")
    ):
        return "Homologacao de acordo"
    if "homolog" in text:
        return "Homologacao"
    if any(
        trecho in text
        for trecho in (
            "ordem concedida",
            "seguranca concedida",
            "concedida a seguranca",
        )
    ):
        return "Ordem/Seguranca concedida"
    if any(
        trecho in text
        for trecho in (
            "ordem denegada",
            "seguranca denegada",
            "denegada a seguranca",
        )
    ):
        return "Ordem/Seguranca denegada"
    if any(trecho in text for trecho in ("absolvicao sumaria", "absolvido sumariamente")):
        return "Absolvicao sumaria"
    if any(trecho in text for trecho in ("absolv", "absolvido", "absolver")):
        return "Absolvicao"
    if any(trecho in text for trecho in ("conden", "condenatoria")):
        return "Condenacao"
    if any(
        trecho in text
        for trecho in (
            "extinta a punibilidade",
            "extincao da punibilidade",
        )
    ):
        return "Extincao da punibilidade"
    if any(trecho in text for trecho in ("extint", "arquiv")):
        return "Extincao/Arquivamento"
    if any(
        trecho in text
        for trecho in (
            "recebida a denuncia",
            "recebimento da denuncia",
            "recebida a queixa",
            "recebimento da queixa",
        )
    ):
        return "Recebimento da denuncia/queixa"
    if any(
        trecho in text
        for trecho in (
            "nao recebida a denuncia",
            "não recebida a denuncia",
            "nao recebimento da denuncia",
            "não recebimento da denuncia",
            "rejeicao da denuncia",
            "rejeição da denuncia",
            "rejeicao da queixa",
            "rejeição da queixa",
        )
    ):
        return "Rejeicao da denuncia/queixa"
    if any(trecho in text for trecho in ("despronuncia", "despronunciado")):
        return "Despronuncia"
    if any(trecho in text for trecho in ("impronuncia", "impronunciado")):
        return "Impronuncia"
    if any(trecho in text for trecho in ("pronuncia", "pronunciado")):
        return "Pronuncia"
    if any(trecho in text for trecho in ("deferid", "concedid")) and any(
        trecho in text for trecho in ("liminar", "tutela", "seguranca")
    ):
        return "Tutela/Liminar concedida"
    if any(trecho in text for trecho in ("indeferid", "negad", "denegad")) and any(
        trecho in text for trecho in ("liminar", "tutela", "seguranca")
    ):
        return "Tutela/Liminar negada"
    if any(
        trecho in text
        for trecho in (
            "nao homolog",
            "não homolog",
            "rejeit",
            "recus",
        )
    ):
        return "Rejeicao/Negativa"
    if any(
        trecho in text
        for trecho in (
            "nao acolh",
            "não acolh",
            "nao conhecimento",
            "não conhecimento",
        )
    ):
        return "Nao acolhimento"
    if any(trecho in text for trecho in ("acolh", "acolhido")):
        return "Acolhimento"
    if any(trecho in text for trecho in ("indefer", "denegad")):
        return "Indeferimento"
    if any(trecho in text for trecho in ("defer", "admit", "recebimento", "recebido", "recebida", "receb")):
        return "Recebimento/Admissao"
    if any(trecho in text for trecho in ("nao conhecid", "prejudicad", "deserto")):
        return "Nao conhecido/Prejudicado"
    return ""


def extract_latest_decision_proxy(movimentos: Any) -> tuple[str, str, Any]:
    if not isinstance(movimentos, list):
        return "", "", pd.NaT

    ultimo_movimento_nome = ""
    ultimo_movimento_data = pd.NaT
    fallback_nome = ""
    fallback_data = pd.NaT

    for movimento in movimentos:
        if not isinstance(movimento, (list, tuple)) or len(movimento) < 2:
            continue
        nome = str(movimento[1] or "").strip()
        data_hora = movimento[2] if len(movimento) > 2 else pd.NaT
        if not nome or not is_decisive_movement_name(nome):
            if not ultimo_movimento_nome and nome:
                ultimo_movimento_nome = nome
                ultimo_movimento_data = data_hora
            continue
        categoria = classify_decision_outcome(nome)
        if categoria:
            return categoria, nome, data_hora
        if not fallback_nome:
            fallback_nome = nome
            fallback_data = data_hora
        if not ultimo_movimento_nome and nome:
            ultimo_movimento_nome = nome
            ultimo_movimento_data = data_hora

    if fallback_nome:
        return CATEGORIA_NAO_CLASSIFICADA, fallback_nome, fallback_data
    if ultimo_movimento_nome:
        return "", ultimo_movimento_nome, ultimo_movimento_data

    return "", "", pd.NaT


def enrich_decision_proxy_dataframe(df_anpp: pd.DataFrame) -> pd.DataFrame:
    if df_anpp.empty or "movimentos" not in df_anpp.columns:
        return pd.DataFrame(
            columns=[
                *list(df_anpp.columns),
                "decisao_categoria",
                "decisao_movimento",
                "decisao_data",
                "dias_ate_decisao_proxy",
            ]
        )

    df_decisao = df_anpp.copy()
    extra = df_decisao["movimentos"].apply(extract_latest_decision_proxy)
    extra_df = pd.DataFrame(
        extra.tolist(),
        columns=["decisao_categoria", "decisao_movimento", "decisao_data"],
        index=df_decisao.index,
    )
    df_decisao = pd.concat([df_decisao, extra_df], axis=1)
    df_decisao["decisao_data"] = pd.to_datetime(df_decisao["decisao_data"], errors="coerce")
    df_decisao["dias_ate_decisao_proxy"] = (
        df_decisao["decisao_data"] - df_decisao["data_ajuizamento"]
    ).dt.total_seconds() / 86400.0
    df_decisao.loc[df_decisao["dias_ate_decisao_proxy"] < 0, "dias_ate_decisao_proxy"] = pd.NA
    return df_decisao


def filter_dataframe_by_tema(df_anpp: pd.DataFrame, tema: str) -> pd.DataFrame:
    if df_anpp.empty or not tema or "assuntos" not in df_anpp.columns:
        return df_anpp.copy()
    mask = df_anpp["assuntos"].apply(
        lambda assuntos: tema in assuntos if isinstance(assuntos, list) else False
    )
    return df_anpp.loc[mask].copy()


def decision_outcomes_dataframe(df_anpp: pd.DataFrame, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "decisao_categoria" not in df_anpp.columns:
        return pd.DataFrame(columns=["desfecho", "quantidade"])

    base = df_anpp["decisao_categoria"].fillna("").astype(str).str.strip()
    base = base[base != ""]
    if base.empty:
        return pd.DataFrame(columns=["desfecho", "quantidade"])

    return base.value_counts().head(max_items).rename_axis("desfecho").reset_index(name="quantidade")


def decision_movements_dataframe(df_anpp: pd.DataFrame, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "decisao_movimento" not in df_anpp.columns:
        return pd.DataFrame(columns=["movimento", "quantidade"])

    base = df_anpp["decisao_movimento"].fillna("").astype(str).str.strip()
    base = base[base != ""]
    if base.empty:
        return pd.DataFrame(columns=["movimento", "quantidade"])

    return base.value_counts().head(max_items).rename_axis("movimento").reset_index(name="quantidade")


def decision_by_orgao_dataframe(df_anpp: pd.DataFrame, max_orgaos: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "orgao_julgador" not in df_anpp.columns:
        return pd.DataFrame(
            columns=[
                "orgao_julgador",
                "processos_tema",
                "com_desfecho",
                "cobertura",
                "desfecho_predominante",
                "forca_predominante",
                "mediana_dias",
            ]
        )

    base = df_anpp.copy()
    base["orgao_julgador"] = base["orgao_julgador"].fillna("").astype(str).str.strip()
    base = base[base["orgao_julgador"] != ""]
    if base.empty:
        return pd.DataFrame(
            columns=[
                "orgao_julgador",
                "processos_tema",
                "com_desfecho",
                "cobertura",
                "desfecho_predominante",
                "forca_predominante",
                "mediana_dias",
            ]
        )

    top_orgaos = base["orgao_julgador"].value_counts().head(max_orgaos).index.tolist()
    base = base[base["orgao_julgador"].isin(top_orgaos)]

    linhas: list[dict[str, Any]] = []
    for orgao in top_orgaos:
        grupo = base[base["orgao_julgador"] == orgao]
        total = len(grupo)
        com_desfecho = grupo["decisao_categoria"].fillna("").astype(str).str.strip()
        com_desfecho = com_desfecho[com_desfecho != ""]
        qtd_desfecho = len(com_desfecho)
        cobertura = f"{(qtd_desfecho / total * 100):.1f}%" if total else "0.0%"
        if qtd_desfecho:
            contagem = com_desfecho.value_counts()
            desfecho_predominante = str(contagem.index[0])
            forca_predominante = f"{(contagem.iloc[0] / qtd_desfecho * 100):.1f}%"
        else:
            desfecho_predominante = "Sem leitura"
            forca_predominante = "-"

        dias = pd.to_numeric(grupo["dias_ate_decisao_proxy"], errors="coerce").dropna()
        mediana_dias = round(float(dias.median()), 1) if not dias.empty else pd.NA

        linhas.append(
            {
                "orgao_julgador": orgao,
                "processos_tema": total,
                "com_desfecho": qtd_desfecho,
                "cobertura": cobertura,
                "desfecho_predominante": desfecho_predominante,
                "forca_predominante": forca_predominante,
                "mediana_dias": mediana_dias,
            }
        )

    return pd.DataFrame(linhas)


def decision_signal_base_dataframe(df_anpp: pd.DataFrame) -> pd.DataFrame:
    if df_anpp.empty or "orgao_julgador" not in df_anpp.columns or "decisao_categoria" not in df_anpp.columns:
        return pd.DataFrame(columns=["orgao_julgador", "decisao_categoria"])

    base = df_anpp[["orgao_julgador", "decisao_categoria"]].copy()
    base["orgao_julgador"] = base["orgao_julgador"].fillna("").astype(str).str.strip()
    base["decisao_categoria"] = base["decisao_categoria"].fillna("").astype(str).str.strip()
    base = base[(base["orgao_julgador"] != "") & (base["decisao_categoria"] != "")]
    if base.empty:
        return pd.DataFrame(columns=["orgao_julgador", "decisao_categoria"])

    base_sem_generico = base[base["decisao_categoria"] != CATEGORIA_NAO_CLASSIFICADA]
    if not base_sem_generico.empty:
        return base_sem_generico
    return base


def outcome_polarity_label(categoria: Any) -> str:
    categoria_limpa = str(categoria or "").strip()
    if not categoria_limpa:
        return POLARIDADE_INDEFINIDA
    return POLARIDADE_DESFECHO_MAP.get(categoria_limpa, POLARIDADE_INDEFINIDA)


def decision_category_series(df_anpp: pd.DataFrame) -> pd.Series:
    if df_anpp.empty or "decisao_categoria" not in df_anpp.columns:
        return pd.Series(dtype="object")
    categorias = df_anpp["decisao_categoria"].fillna("").astype(str).str.strip()
    categorias = categorias[categorias != ""]
    if categorias.empty:
        return pd.Series(dtype="object")
    categorias_sem_generico = categorias[categorias != CATEGORIA_NAO_CLASSIFICADA]
    if not categorias_sem_generico.empty:
        return categorias_sem_generico
    return categorias


def favorability_index_from_counts(
    favoravel: int,
    desfavoravel: int,
    misto: int,
) -> float | None:
    total_util = favoravel + desfavoravel + misto
    if total_util <= 0:
        return None
    indice = ((favoravel + (0.5 * misto)) - desfavoravel) / total_util * 100
    return float(indice)


def favorability_label_from_index(indice: float | None) -> str:
    if indice is None:
        return "Sem base"
    if indice >= 40:
        return "Muito favoravel"
    if indice >= 15:
        return "Favoravel"
    if indice > -15:
        return "Equilibrado"
    if indice > -40:
        return "Restritivo"
    return "Muito restritivo"


def stability_index_from_counts(contagem: pd.Series) -> float | None:
    total = float(pd.to_numeric(contagem, errors="coerce").fillna(0).sum())
    if total <= 0:
        return None
    shares = pd.to_numeric(contagem, errors="coerce").fillna(0) / total
    return float((shares.pow(2).sum()) * 100)


def stability_label_from_index(indice: float | None) -> str:
    if indice is None:
        return "Sem base"
    if indice >= 80:
        return "Muito alta"
    if indice >= 60:
        return "Alta"
    if indice >= 45:
        return "Media"
    return "Baixa"


def decision_polarity_base_dataframe(df_anpp: pd.DataFrame) -> pd.DataFrame:
    base = decision_signal_base_dataframe(df_anpp)
    if base.empty:
        return pd.DataFrame(columns=["orgao_julgador", "decisao_categoria", "polaridade"])
    base = base.copy()
    base["polaridade"] = base["decisao_categoria"].apply(outcome_polarity_label)
    return base


def decision_favorability_summary(df_anpp: pd.DataFrame) -> dict[str, Any]:
    categorias = decision_category_series(df_anpp)
    if categorias.empty:
        return {
            "total_classificados": 0,
            "decisoes_uteis": 0,
            "favoravel_qtd": 0,
            "desfavoravel_qtd": 0,
            "misto_qtd": 0,
            "neutro_qtd": 0,
            "favoravel_pct": 0.0,
            "desfavoravel_pct": 0.0,
            "misto_pct": 0.0,
            "neutro_pct": 0.0,
            "indice_favorabilidade": None,
            "leitura_favorabilidade": "Sem base",
        }

    polaridades = categorias.apply(outcome_polarity_label).value_counts()
    total_classificados = int(len(categorias))
    favoravel_qtd = int(polaridades.get(POLARIDADE_FAVORAVEL, 0))
    desfavoravel_qtd = int(polaridades.get(POLARIDADE_DESFAVORAVEL, 0))
    misto_qtd = int(polaridades.get(POLARIDADE_MISTA, 0))
    neutro_qtd = int(
        polaridades.get(POLARIDADE_NEUTRA, 0) + polaridades.get(POLARIDADE_INDEFINIDA, 0)
    )
    decisoes_uteis = favoravel_qtd + desfavoravel_qtd + misto_qtd
    indice = favorability_index_from_counts(favoravel_qtd, desfavoravel_qtd, misto_qtd)

    return {
        "total_classificados": total_classificados,
        "decisoes_uteis": decisoes_uteis,
        "favoravel_qtd": favoravel_qtd,
        "desfavoravel_qtd": desfavoravel_qtd,
        "misto_qtd": misto_qtd,
        "neutro_qtd": neutro_qtd,
        "favoravel_pct": (favoravel_qtd / decisoes_uteis * 100) if decisoes_uteis else 0.0,
        "desfavoravel_pct": (desfavoravel_qtd / decisoes_uteis * 100) if decisoes_uteis else 0.0,
        "misto_pct": (misto_qtd / decisoes_uteis * 100) if decisoes_uteis else 0.0,
        "neutro_pct": (neutro_qtd / total_classificados * 100) if total_classificados else 0.0,
        "indice_favorabilidade": indice,
        "leitura_favorabilidade": favorability_label_from_index(indice),
    }


def decision_stability_summary(df_anpp: pd.DataFrame) -> dict[str, Any]:
    categorias = decision_category_series(df_anpp)
    if categorias.empty:
        return {
            "total_classificados": 0,
            "desfecho_lider": "",
            "forca_lider": 0.0,
            "indice_estabilidade": None,
            "perfil_estabilidade": "Sem base",
        }

    contagem = categorias.value_counts()
    indice = stability_index_from_counts(contagem)
    lider = str(contagem.index[0]) if not contagem.empty else ""
    forca_lider = (float(contagem.iloc[0]) / float(contagem.sum()) * 100) if not contagem.empty else 0.0
    return {
        "total_classificados": int(contagem.sum()),
        "desfecho_lider": lider,
        "forca_lider": forca_lider,
        "indice_estabilidade": indice,
        "perfil_estabilidade": stability_label_from_index(indice),
    }


def decision_favorability_by_orgao_dataframe(
    df_anpp: pd.DataFrame,
    min_decisoes_uteis: int = 5,
    max_items: int | None = 12,
) -> pd.DataFrame:
    base = decision_polarity_base_dataframe(df_anpp)
    if base.empty:
        return pd.DataFrame(
            columns=[
                "orgao_julgador",
                "decisoes_classificadas",
                "decisoes_uteis",
                "favoravel_pct",
                "desfavoravel_pct",
                "misto_pct",
                "indice_favorabilidade",
                "leitura_favorabilidade",
            ]
        )

    linhas: list[dict[str, Any]] = []
    for orgao, grupo in base.groupby("orgao_julgador"):
        polaridades = grupo["polaridade"].value_counts()
        favoravel = int(polaridades.get(POLARIDADE_FAVORAVEL, 0))
        desfavoravel = int(polaridades.get(POLARIDADE_DESFAVORAVEL, 0))
        misto = int(polaridades.get(POLARIDADE_MISTA, 0))
        decisoes_uteis = favoravel + desfavoravel + misto
        if decisoes_uteis < min_decisoes_uteis:
            continue
        indice = favorability_index_from_counts(favoravel, desfavoravel, misto)
        linhas.append(
            {
                "orgao_julgador": orgao,
                "decisoes_classificadas": int(len(grupo)),
                "decisoes_uteis": decisoes_uteis,
                "favoravel_pct": round((favoravel / decisoes_uteis * 100), 1) if decisoes_uteis else 0.0,
                "desfavoravel_pct": round((desfavoravel / decisoes_uteis * 100), 1) if decisoes_uteis else 0.0,
                "misto_pct": round((misto / decisoes_uteis * 100), 1) if decisoes_uteis else 0.0,
                "indice_favorabilidade": round(float(indice), 1) if indice is not None else pd.NA,
                "leitura_favorabilidade": favorability_label_from_index(indice),
            }
        )

    if not linhas:
        return pd.DataFrame(
            columns=[
                "orgao_julgador",
                "decisoes_classificadas",
                "decisoes_uteis",
                "favoravel_pct",
                "desfavoravel_pct",
                "misto_pct",
                "indice_favorabilidade",
                "leitura_favorabilidade",
            ]
        )

    resultado = pd.DataFrame(linhas).sort_values(
        ["indice_favorabilidade", "decisoes_uteis"], ascending=[False, False]
    )
    if max_items is not None:
        resultado = resultado.head(max_items)
    return resultado.reset_index(drop=True)


def decision_time_by_orgao_dataframe(
    df_anpp: pd.DataFrame,
    min_processos: int = 3,
    max_items: int | None = 12,
) -> pd.DataFrame:
    if df_anpp.empty or "orgao_julgador" not in df_anpp.columns or "dias_ate_decisao_proxy" not in df_anpp.columns:
        return pd.DataFrame(columns=["orgao_julgador", "processos_com_tempo", "mediana_dias", "p75_dias"])

    base = df_anpp[["orgao_julgador", "dias_ate_decisao_proxy"]].copy()
    base["orgao_julgador"] = base["orgao_julgador"].fillna("").astype(str).str.strip()
    base["dias_ate_decisao_proxy"] = pd.to_numeric(base["dias_ate_decisao_proxy"], errors="coerce")
    base = base[(base["orgao_julgador"] != "") & (base["dias_ate_decisao_proxy"].notna())]
    if base.empty:
        return pd.DataFrame(columns=["orgao_julgador", "processos_com_tempo", "mediana_dias", "p75_dias"])

    linhas: list[dict[str, Any]] = []
    for orgao, grupo in base.groupby("orgao_julgador"):
        if len(grupo) < min_processos:
            continue
        linhas.append(
            {
                "orgao_julgador": orgao,
                "processos_com_tempo": int(len(grupo)),
                "mediana_dias": round(float(grupo["dias_ate_decisao_proxy"].median()), 1),
                "p75_dias": round(float(grupo["dias_ate_decisao_proxy"].quantile(0.75)), 1),
            }
        )

    if not linhas:
        return pd.DataFrame(columns=["orgao_julgador", "processos_com_tempo", "mediana_dias", "p75_dias"])

    resultado = pd.DataFrame(linhas).sort_values(
        ["mediana_dias", "processos_com_tempo"], ascending=[True, False]
    )
    if max_items is not None:
        resultado = resultado.head(max_items)
    return resultado.reset_index(drop=True)


def decision_stability_by_orgao_dataframe(
    df_anpp: pd.DataFrame,
    min_classificados: int = 5,
    max_items: int | None = 12,
) -> pd.DataFrame:
    base = decision_signal_base_dataframe(df_anpp)
    if base.empty:
        return pd.DataFrame(
            columns=[
                "orgao_julgador",
                "decisoes_classificadas",
                "desfecho_lider",
                "forca_lider",
                "indice_estabilidade",
                "perfil_estabilidade",
            ]
        )

    linhas: list[dict[str, Any]] = []
    for orgao, grupo in base.groupby("orgao_julgador"):
        if len(grupo) < min_classificados:
            continue
        contagem = grupo["decisao_categoria"].value_counts()
        indice = stability_index_from_counts(contagem)
        linhas.append(
            {
                "orgao_julgador": orgao,
                "decisoes_classificadas": int(len(grupo)),
                "desfecho_lider": str(contagem.index[0]),
                "forca_lider": round(float(contagem.iloc[0] / contagem.sum() * 100), 1),
                "indice_estabilidade": round(float(indice), 1) if indice is not None else pd.NA,
                "perfil_estabilidade": stability_label_from_index(indice),
            }
        )

    if not linhas:
        return pd.DataFrame(
            columns=[
                "orgao_julgador",
                "decisoes_classificadas",
                "desfecho_lider",
                "forca_lider",
                "indice_estabilidade",
                "perfil_estabilidade",
            ]
        )

    resultado = pd.DataFrame(linhas).sort_values(
        ["indice_estabilidade", "decisoes_classificadas"], ascending=[False, False]
    )
    if max_items is not None:
        resultado = resultado.head(max_items)
    return resultado.reset_index(drop=True)


def decision_pattern_change_summary(df_anpp: pd.DataFrame) -> dict[str, Any]:
    if (
        df_anpp.empty
        or "decisao_categoria" not in df_anpp.columns
        or "decisao_data" not in df_anpp.columns
    ):
        return {
            "janela_meses": 0,
            "meses_recentes": [],
            "meses_anteriores": [],
            "qtd_recente": 0,
            "qtd_anterior": 0,
            "desfecho_lider_recente": "",
            "desfecho_lider_anterior": "",
            "indice_recente": None,
            "indice_anterior": None,
            "delta_indice": None,
            "mudanca_principal": "Sem base",
        }

    base = df_anpp[["decisao_categoria", "decisao_data"]].copy()
    base["decisao_categoria"] = base["decisao_categoria"].fillna("").astype(str).str.strip()
    base["decisao_data"] = pd.to_datetime(base["decisao_data"], errors="coerce")
    base = base[(base["decisao_categoria"] != "") & (base["decisao_data"].notna())]
    if base.empty:
        return {
            "janela_meses": 0,
            "meses_recentes": [],
            "meses_anteriores": [],
            "qtd_recente": 0,
            "qtd_anterior": 0,
            "desfecho_lider_recente": "",
            "desfecho_lider_anterior": "",
            "indice_recente": None,
            "indice_anterior": None,
            "delta_indice": None,
            "mudanca_principal": "Sem base",
        }

    base["mes_decisao"] = base["decisao_data"].dt.to_period("M")
    meses = sorted(base["mes_decisao"].dropna().unique())
    janela = 0
    if len(meses) >= 6:
        janela = 3
    elif len(meses) >= 4:
        janela = 2
    elif len(meses) >= 2:
        janela = 1
    if janela == 0:
        return {
            "janela_meses": 0,
            "meses_recentes": [],
            "meses_anteriores": [],
            "qtd_recente": 0,
            "qtd_anterior": 0,
            "desfecho_lider_recente": "",
            "desfecho_lider_anterior": "",
            "indice_recente": None,
            "indice_anterior": None,
            "delta_indice": None,
            "mudanca_principal": "Sem base",
        }

    meses_recentes = meses[-janela:]
    meses_anteriores = meses[-(janela * 2):-janela]
    base_recente = base[base["mes_decisao"].isin(meses_recentes)].copy()
    base_anterior = base[base["mes_decisao"].isin(meses_anteriores)].copy()

    contagem_recente = base_recente["decisao_categoria"].value_counts()
    contagem_anterior = base_anterior["decisao_categoria"].value_counts()
    lider_recente = str(contagem_recente.index[0]) if not contagem_recente.empty else ""
    lider_anterior = str(contagem_anterior.index[0]) if not contagem_anterior.empty else ""

    favor_recente = decision_favorability_summary(base_recente)
    favor_anterior = decision_favorability_summary(base_anterior)
    indice_recente = favor_recente["indice_favorabilidade"]
    indice_anterior = favor_anterior["indice_favorabilidade"]
    delta_indice = (
        float(indice_recente) - float(indice_anterior)
        if indice_recente is not None and indice_anterior is not None
        else None
    )

    mudanca_principal = "Padrao semelhante"
    if lider_recente and lider_anterior and lider_recente != lider_anterior:
        mudanca_principal = "Mudanca do desfecho lider"
    elif delta_indice is not None and delta_indice >= 12:
        mudanca_principal = "Sinal mais favoravel"
    elif delta_indice is not None and delta_indice <= -12:
        mudanca_principal = "Sinal mais restritivo"

    return {
        "janela_meses": janela,
        "meses_recentes": [mes.strftime("%m/%Y") for mes in meses_recentes],
        "meses_anteriores": [mes.strftime("%m/%Y") for mes in meses_anteriores],
        "qtd_recente": int(len(base_recente)),
        "qtd_anterior": int(len(base_anterior)),
        "desfecho_lider_recente": lider_recente,
        "desfecho_lider_anterior": lider_anterior,
        "indice_recente": indice_recente,
        "indice_anterior": indice_anterior,
        "delta_indice": delta_indice,
        "mudanca_principal": mudanca_principal,
    }


def theme_sample_alerts(
    total_tema: int,
    total_com_desfecho: int,
    favorabilidade_tema: dict[str, Any],
    favorabilidade_orgaos: pd.DataFrame,
    tempo_orgaos: pd.DataFrame,
    mudanca_padrao: dict[str, Any],
) -> list[str]:
    alertas: list[str] = []
    if total_tema < 30:
        alertas.append(
            "A amostra total do tema ainda e pequena. Use os rankings como indicio inicial, nao como padrao fechado."
        )
    if total_com_desfecho < 10:
        alertas.append(
            "Poucos processos do tema tiveram desfecho classificado automaticamente. Isso reduz a seguranca da leitura comparativa."
        )
    if int(favorabilidade_tema.get("decisoes_uteis", 0) or 0) < 10:
        alertas.append(
            "A favorabilidade estimada ainda tem pouca base util. Desfechos neutros ou processuais estao pesando mais do que o ideal."
        )
    if not isinstance(favorabilidade_orgaos, pd.DataFrame) or len(favorabilidade_orgaos) < 3:
        alertas.append(
            "Ainda nao ha orgaos suficientes com base util para um ranking robusto de favorabilidade."
        )
    if not isinstance(tempo_orgaos, pd.DataFrame) or tempo_orgaos.empty:
        alertas.append(
            "Ainda nao ha massa critica para comparar tempo mediano de decisao por orgao neste tema."
        )
    if int(mudanca_padrao.get("janela_meses", 0) or 0) == 0:
        alertas.append(
            "A serie decisoria recente do tema ainda nao tem meses suficientes para medir mudanca de padrao."
        )
    return alertas


def decision_outcome_mix_by_orgao_dataframe(
    df_anpp: pd.DataFrame,
    max_orgaos: int = 8,
    max_desfechos: int = 5,
) -> pd.DataFrame:
    base = decision_signal_base_dataframe(df_anpp)
    if base.empty:
        return pd.DataFrame(columns=["orgao_julgador", "total_classificados"])

    top_orgaos = base["orgao_julgador"].value_counts().head(max_orgaos).index.tolist()
    top_desfechos = base["decisao_categoria"].value_counts().head(max_desfechos).index.tolist()
    base = base[base["orgao_julgador"].isin(top_orgaos) & base["decisao_categoria"].isin(top_desfechos)]
    if base.empty:
        return pd.DataFrame(columns=["orgao_julgador", "total_classificados"])

    pivot = pd.crosstab(base["orgao_julgador"], base["decisao_categoria"])
    pivot = pivot.reindex(index=top_orgaos, columns=top_desfechos, fill_value=0)
    pivot["total_classificados"] = pivot.sum(axis=1)
    pivot = pivot.reset_index()
    return pivot


def decision_coverage_summary(df_anpp: pd.DataFrame) -> dict[str, Any]:
    if df_anpp.empty:
        return {
            "total_processos": 0,
            "com_desfecho": 0,
            "com_movimento_final": 0,
            "cobertura_desfecho": 0.0,
            "cobertura_movimento": 0.0,
        }

    total = len(df_anpp)
    com_desfecho = int(
        df_anpp["decisao_categoria"].fillna("").astype(str).str.strip().ne("").sum()
    ) if "decisao_categoria" in df_anpp.columns else 0
    com_movimento_final = int(
        df_anpp["decisao_movimento"].fillna("").astype(str).str.strip().ne("").sum()
    ) if "decisao_movimento" in df_anpp.columns else 0
    return {
        "total_processos": total,
        "com_desfecho": com_desfecho,
        "com_movimento_final": com_movimento_final,
        "cobertura_desfecho": (com_desfecho / total * 100) if total else 0.0,
        "cobertura_movimento": (com_movimento_final / total * 100) if total else 0.0,
    }


def theme_sample_strength_label(total_processos: int, com_desfecho: int) -> str:
    if total_processos >= 100 and com_desfecho >= 30:
        return "Alta"
    if total_processos >= 30 and com_desfecho >= 10:
        return "Media"
    return "Baixa"


def theme_concentration_summary(df_anpp: pd.DataFrame) -> dict[str, Any]:
    if df_anpp.empty or "orgao_julgador" not in df_anpp.columns:
        return {
            "total_com_orgao": 0,
            "top_orgao": "",
            "top_orgao_qtd": 0,
            "top_orgao_share": 0.0,
            "top3_qtd": 0,
            "top3_share": 0.0,
        }

    orgaos = df_anpp["orgao_julgador"].fillna("").astype(str).str.strip()
    orgaos = orgaos[orgaos != ""]
    if orgaos.empty:
        return {
            "total_com_orgao": 0,
            "top_orgao": "",
            "top_orgao_qtd": 0,
            "top_orgao_share": 0.0,
            "top3_qtd": 0,
            "top3_share": 0.0,
        }

    contagem = orgaos.value_counts()
    total = int(contagem.sum())
    top_orgao = str(contagem.index[0])
    top_orgao_qtd = int(contagem.iloc[0])
    top3_qtd = int(contagem.head(3).sum())
    return {
        "total_com_orgao": total,
        "top_orgao": top_orgao,
        "top_orgao_qtd": top_orgao_qtd,
        "top_orgao_share": (top_orgao_qtd / total * 100) if total else 0.0,
        "top3_qtd": top3_qtd,
        "top3_share": (top3_qtd / total * 100) if total else 0.0,
    }


def theme_monthly_counts(df_anpp: pd.DataFrame, max_meses: int = 12) -> pd.Series:
    return monthly_counts(df_anpp, max_meses=max_meses)


def theme_recent_trend_summary(df_anpp: pd.DataFrame, max_meses: int = 12) -> dict[str, Any]:
    serie = theme_monthly_counts(df_anpp, max_meses=max_meses)
    if serie.empty:
        return {
            "serie": serie,
            "meses_base": 0,
            "ultimo_mes": "",
            "ultimo_valor": 0,
            "variacao_pct": None,
            "tendencia": "Sem dados",
        }

    ultimo_idx = serie.index[-1]
    ultimo_valor = int(serie.iloc[-1])
    variacao_pct = None
    tendencia = "Estavel"
    if len(serie) >= 2:
        penultimo_valor = int(serie.iloc[-2])
        if penultimo_valor > 0:
            variacao_pct = ((int(serie.iloc[-1]) - penultimo_valor) / penultimo_valor) * 100
            if variacao_pct > 5:
                tendencia = "Alta recente"
            elif variacao_pct < -5:
                tendencia = "Queda recente"
        elif ultimo_valor > 0:
            tendencia = "Alta recente"
    return {
        "serie": serie,
        "meses_base": len(serie),
        "ultimo_mes": ultimo_idx.strftime("%m/%Y"),
        "ultimo_valor": ultimo_valor,
        "variacao_pct": variacao_pct,
        "tendencia": tendencia,
    }


def related_themes_dataframe(df_anpp: pd.DataFrame, tema: str, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "assuntos" not in df_anpp.columns or not tema:
        return pd.DataFrame(columns=["tema_relacionado", "quantidade"])

    relacionados: list[str] = []
    for assuntos in df_anpp["assuntos"]:
        if not isinstance(assuntos, list) or tema not in assuntos:
            continue
        for assunto in assuntos:
            if not assunto or assunto == tema:
                continue
            relacionados.append(str(assunto).strip())

    if not relacionados:
        return pd.DataFrame(columns=["tema_relacionado", "quantidade"])

    return (
        pd.Series(relacionados)
        .value_counts()
        .head(max_items)
        .rename_axis("tema_relacionado")
        .reset_index(name="quantidade")
    )


def theme_overview_dataframe(df_anpp: pd.DataFrame, max_items: int = 15) -> pd.DataFrame:
    if df_anpp.empty or "assuntos" not in df_anpp.columns:
        return pd.DataFrame(
            columns=[
                "tema",
                "processos",
                "com_desfecho",
                "cobertura_desfecho",
                "com_movimento_final",
                "cobertura_movimento",
            ]
        )

    linhas: list[dict[str, Any]] = []
    for _, row in df_anpp.iterrows():
        assuntos = row.get("assuntos", [])
        if not isinstance(assuntos, list):
            continue
        temas = []
        for assunto in assuntos:
            tema = str(assunto or "").strip()
            if tema:
                temas.append(tema)
        temas = list(dict.fromkeys(temas))
        if not temas:
            continue
        tem_desfecho = bool(str(row.get("decisao_categoria", "") or "").strip())
        tem_movimento = bool(str(row.get("decisao_movimento", "") or "").strip())
        for tema in temas:
            linhas.append(
                {
                    "tema": tema,
                    "processos": 1,
                    "com_desfecho": 1 if tem_desfecho else 0,
                    "com_movimento_final": 1 if tem_movimento else 0,
                }
            )

    if not linhas:
        return pd.DataFrame(
            columns=[
                "tema",
                "processos",
                "com_desfecho",
                "cobertura_desfecho",
                "com_movimento_final",
                "cobertura_movimento",
            ]
        )

    overview = (
        pd.DataFrame(linhas)
        .groupby("tema", as_index=False)
        .sum()
        .sort_values(["processos", "com_desfecho"], ascending=[False, False])
        .head(max_items)
    )
    overview["cobertura_desfecho"] = (
        (overview["com_desfecho"] / overview["processos"] * 100)
        .round(1)
        .map(lambda valor: f"{valor:.1f}%")
    )
    overview["cobertura_movimento"] = (
        (overview["com_movimento_final"] / overview["processos"] * 100)
        .round(1)
        .map(lambda valor: f"{valor:.1f}%")
    )
    return overview[
        [
            "tema",
            "processos",
            "com_desfecho",
            "cobertura_desfecho",
            "com_movimento_final",
            "cobertura_movimento",
        ]
    ]


class DataJudRequestError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def build_datajud_error_message(
    status_code: int | None,
    size: int,
    numero_processo: str = "",
    data_inicio: Any = None,
    data_fim: Any = None,
) -> str:
    contexto = "a consulta"
    if numero_processo:
        contexto = "a consulta por numero do processo"
    elif size:
        contexto = f"a consulta da amostra de {size:,} registros".replace(",", ".")

    periodo = format_periodo_aplicado(data_inicio, data_fim)
    periodo_texto = f" no periodo {periodo}" if periodo else ""

    if status_code == 401:
        return (
            "401 Unauthorized: chave API invalida, expirada ou sem permissao para este endpoint. "
            "Use no formato 'APIKey ...'."
        )
    if status_code == 429:
        return (
            f"O DataJud recusou temporariamente {contexto}{periodo_texto} por excesso de requisicoes. "
            "Espere alguns instantes e tente novamente."
        )
    if status_code in DATAJUD_RETRYABLE_STATUS:
        return (
            f"O DataJud demorou demais para responder {contexto}{periodo_texto}. "
            "O app tentou novamente automaticamente, mas a API continuou lenta. "
            "Tente reduzir a quantidade, aplicar um periodo menor ou repetir a consulta em alguns minutos."
        )
    if status_code:
        return (
            f"O DataJud retornou erro {status_code} para {contexto}{periodo_texto}. "
            "Tente novamente em instantes."
        )
    return (
        f"Nao foi possivel concluir {contexto}{periodo_texto} por instabilidade na comunicacao com o DataJud. "
        "Tente novamente em alguns minutos."
    )


def post_datajud_with_retry(
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    size: int,
    numero_processo: str = "",
    data_inicio: Any = None,
    data_fim: Any = None,
) -> requests.Response:
    last_error: Exception | None = None
    for tentativa in range(DATAJUD_MAX_RETRIES + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=DATAJUD_TIMEOUT_SECONDS,
            )
            if response.status_code in DATAJUD_RETRYABLE_STATUS and tentativa < DATAJUD_MAX_RETRIES:
                time.sleep(1.2 * (tentativa + 1))
                continue
            response.raise_for_status()
            return response
        except Timeout as exc:
            last_error = exc
            if tentativa < DATAJUD_MAX_RETRIES:
                time.sleep(1.2 * (tentativa + 1))
                continue
            raise DataJudRequestError(
                build_datajud_error_message(
                    504,
                    size=size,
                    numero_processo=numero_processo,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                ),
                status_code=504,
            ) from exc
        except HTTPError as exc:
            last_error = exc
            status = exc.response.status_code if exc.response is not None else None
            if status in DATAJUD_RETRYABLE_STATUS and tentativa < DATAJUD_MAX_RETRIES:
                time.sleep(1.2 * (tentativa + 1))
                continue
            raise DataJudRequestError(
                build_datajud_error_message(
                    status,
                    size=size,
                    numero_processo=numero_processo,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                ),
                status_code=status,
            ) from exc
        except RequestException as exc:
            last_error = exc
            if tentativa < DATAJUD_MAX_RETRIES:
                time.sleep(1.2 * (tentativa + 1))
                continue
            raise DataJudRequestError(
                build_datajud_error_message(
                    None,
                    size=size,
                    numero_processo=numero_processo,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                ),
                status_code=None,
            ) from exc

    raise DataJudRequestError(
        build_datajud_error_message(
            None,
            size=size,
            numero_processo=numero_processo,
            data_inicio=data_inicio,
            data_fim=data_fim,
        ),
        status_code=None,
    ) from last_error


@st.cache_data(show_spinner=False, ttl=1200)
def fetch_hits(
    api_key: str,
    classe_codigo: int,
    size: int,
    url: str,
    numero_processo: str = "",
    data_inicio: Any = None,
    data_fim: Any = None,
    incluir_movimentos: bool = False,
    modo_consulta: str = "classe_ou_processo",
) -> list[dict[str, Any]]:
    numero_limpo = normalize_numero_processo(numero_processo)
    filtros: list[dict[str, Any]] = []
    if numero_limpo:
        filtros.append({"match": {"numeroProcesso": numero_limpo}})
    elif modo_consulta == "mapa_tribunal":
        pass
    else:
        filtros.append({"match": {"classe.codigo": classe_codigo}})

    filtro_data = build_data_ajuizamento_range(data_inicio=data_inicio, data_fim=data_fim)
    if filtro_data:
        filtros.append(filtro_data)

    if filtros:
        query: dict[str, Any] = {"bool": {"filter": filtros}}
    else:
        query = {"match_all": {}}

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
        response = post_datajud_with_retry(
            url=url,
            headers=headers,
            payload=payload,
            size=size,
            numero_processo=numero_limpo,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )
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

        response = post_datajud_with_retry(
            url=url,
            headers=headers,
            payload=paged_payload,
            size=page_size,
            numero_processo=numero_limpo,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )
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


def top_orgaos_julgadores_dataframe(df_anpp: pd.DataFrame, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "orgao_julgador" not in df_anpp.columns:
        return pd.DataFrame(columns=["orgao_julgador", "quantidade", "participacao"])

    orgaos = df_anpp["orgao_julgador"].fillna("").astype(str).str.strip()
    orgaos = orgaos[orgaos != ""]
    if orgaos.empty:
        return pd.DataFrame(columns=["orgao_julgador", "quantidade", "participacao"])

    total = int(len(orgaos))
    top = (
        orgaos.value_counts()
        .head(max_items)
        .rename_axis("orgao_julgador")
        .reset_index(name="quantidade")
    )
    top["participacao"] = (
        (top["quantidade"] / total * 100)
        .round(1)
        .map(lambda valor: f"{valor:.1f}%")
    )
    return top


def format_int_br(value: Any) -> str:
    try:
        return f"{int(value):,}".replace(",", ".")
    except Exception:
        return str(value)


def render_theme_metric_card(
    column: Any,
    label: str,
    value: Any,
    delta: str | None = None,
) -> None:
    label_html = html.escape(str(label or ""))
    value_html = html.escape(str(value or "-"))
    delta_html = ""
    if delta:
        delta_html = f"<div class='theme-metric-delta'>{html.escape(str(delta))}</div>"
    column.markdown(
        f"""
        <div class="theme-metric-card">
            <div class="theme-metric-label">{label_html}</div>
            <div class="theme-metric-value">{value_html}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_sample_insights(
    df_anpp: pd.DataFrame,
    df_mensal: pd.DataFrame,
    top_orgaos_df: pd.DataFrame,
    top_100_df: pd.DataFrame,
) -> list[str]:
    if df_anpp.empty:
        return ["Sem dados suficientes para gerar insights automáticos da amostra."]

    insights: list[str] = []
    total_registros = len(df_anpp)

    if isinstance(top_orgaos_df, pd.DataFrame) and not top_orgaos_df.empty:
        top_orgao = top_orgaos_df.iloc[0]
        insights.append(
            f"O órgão julgador mais frequente na amostra é `{top_orgao['orgao_julgador']}`, com "
            f"{format_int_br(top_orgao['quantidade'])} registros ({top_orgao['participacao']} da amostra)."
        )
        if len(top_orgaos_df) >= 3:
            top_3 = int(pd.to_numeric(top_orgaos_df.head(3)["quantidade"], errors="coerce").fillna(0).sum())
            pct_top_3 = (top_3 / total_registros * 100) if total_registros else 0.0
            leitura = "concentrada" if pct_top_3 >= 50 else "mais distribuída"
            insights.append(
                f"Os 3 órgãos mais frequentes somam {format_int_br(top_3)} registros ({pct_top_3:.1f}% da amostra), "
                f"o que sugere uma base {leitura} em poucos órgãos julgadores."
            )

    datas = df_anpp["data_ajuizamento"].dropna() if "data_ajuizamento" in df_anpp.columns else pd.Series(dtype="datetime64[ns]")
    if not datas.empty:
        horas = datas.dt.hour.value_counts().sort_index()
        if not horas.empty:
            hora_pico = int(horas.idxmax())
            qtd_hora_pico = int(horas.max())
            pct_hora_pico = (qtd_hora_pico / len(datas) * 100) if len(datas) else 0.0
            insights.append(
                f"O pico de ajuizamentos ocorreu por volta das {hora_pico:02d}h, com "
                f"{format_int_br(qtd_hora_pico)} registros ({pct_hora_pico:.1f}% dos registros com data)."
            )
            expediente = int(horas[(horas.index >= 8) & (horas.index < 19)].sum())
            fora_expediente = int(horas.sum() - expediente)
            pct_expediente = (expediente / int(horas.sum()) * 100) if int(horas.sum()) else 0.0
            if expediente >= fora_expediente:
                insights.append(
                    f"A maior parte dos ajuizamentos ocorreu dentro do horário comercial, com {pct_expediente:.1f}% dos registros com data."
                )
            else:
                insights.append(
                    f"A maior parte dos ajuizamentos ocorreu fora do horário comercial, com {(100 - pct_expediente):.1f}% dos registros com data."
                )

    serie_mensal = monthly_counts(df_mensal, max_meses=12)
    if not serie_mensal.empty:
        pico_idx = serie_mensal.idxmax()
        pico_val = int(serie_mensal.max())
        insights.append(
            f"Nos últimos {len(serie_mensal)} meses disponíveis, o pico foi em {pico_idx.strftime('%m/%Y')}, "
            f"com {format_int_br(pico_val)} ajuizamentos."
        )
        if len(serie_mensal) >= 2:
            ultimo = int(serie_mensal.iloc[-1])
            penultimo = int(serie_mensal.iloc[-2])
            if penultimo > 0:
                variacao = ((ultimo - penultimo) / penultimo) * 100
                if variacao > 0:
                    insights.append(
                        f"O mês mais recente ficou {variacao:.1f}% acima do mês anterior."
                    )
                elif variacao < 0:
                    insights.append(
                        f"O mês mais recente ficou {abs(variacao):.1f}% abaixo do mês anterior."
                    )
                else:
                    insights.append("O mês mais recente ficou no mesmo patamar do mês anterior.")

    top_classe = top_classes_dataframe(df_anpp, max_items=1)
    if not top_classe.empty:
        classe = str(top_classe.iloc[0]["classe"]).strip()
        quantidade = int(top_classe.iloc[0]["quantidade"])
        insights.append(
            f"A classe processual mais frequente na amostra é `{classe}`, com {format_int_br(quantidade)} registros."
        )

    top_assunto = top_assuntos_dataframe(df_anpp, max_items=1)
    if not top_assunto.empty:
        assunto = str(top_assunto.iloc[0]["assunto"]).strip()
        quantidade = int(top_assunto.iloc[0]["quantidade"])
        insights.append(
            f"O assunto mais recorrente é `{assunto}`, com {format_int_br(quantidade)} ocorrências na amostra."
        )

    if (
        isinstance(top_100_df, pd.DataFrame)
        and not top_100_df.empty
        and {"municipio", "orgao_julgador", "quantidade"}.issubset(top_100_df.columns)
    ):
        top_linha = top_100_df.iloc[0]
        insights.append(
            f"A combinação município/órgão mais frequente é `{top_linha['municipio']} / {top_linha['orgao_julgador']}`, "
            f"com {format_int_br(top_linha['quantidade'])} registros."
        )

    return insights[:6]


def build_map_insights(
    top_codigos: pd.DataFrame,
    top_classes: pd.DataFrame,
    top_assuntos: pd.DataFrame,
    qtd_mapa: int,
) -> list[str]:
    insights: list[str] = []
    if qtd_mapa:
        insights.append(
            f"O mapa automático foi montado com até {format_int_br(qtd_mapa)} registros recentes da sigla selecionada."
        )
    if isinstance(top_codigos, pd.DataFrame) and not top_codigos.empty:
        linha = top_codigos.iloc[0]
        insights.append(
            f"O código mais frequente no mapa da sigla é `{linha['classe_codigo']}`, ligado à classe `{linha['classe']}`, "
            f"com {format_int_br(linha['quantidade'])} ocorrências."
        )
    if isinstance(top_classes, pd.DataFrame) and not top_classes.empty:
        linha = top_classes.iloc[0]
        insights.append(
            f"A classe mais frequente no mapa da sigla é `{linha['classe']}`, com {format_int_br(linha['quantidade'])} registros."
        )
    if isinstance(top_assuntos, pd.DataFrame) and not top_assuntos.empty:
        linha = top_assuntos.iloc[0]
        insights.append(
            f"O assunto mais recorrente no mapa da sigla é `{linha['assunto']}`, com {format_int_br(linha['quantidade'])} ocorrências."
        )
    return insights


def build_decision_theme_insights(
    tema: str,
    total_tema: int,
    total_com_desfecho: int,
    desfechos_tema: pd.DataFrame,
    movimentos_tema: pd.DataFrame,
    orgaos_tema: pd.DataFrame,
    forca_tema: str,
    concentracao_tema: dict[str, Any],
    tendencia_tema: dict[str, Any],
    favorabilidade_tema: dict[str, Any],
    estabilidade_tema: dict[str, Any],
    mudanca_padrao: dict[str, Any],
    alertas_tema: list[str],
) -> list[str]:
    if total_tema <= 0:
        return ["Sem dados suficientes para gerar insights automáticos deste tema."]

    insights: list[str] = []
    cobertura = (total_com_desfecho / total_tema * 100) if total_tema else 0.0
    insights.append(
        f"No tema `{tema}`, o app encontrou {format_int_br(total_tema)} processos na amostra, com leitura decisória em {cobertura:.1f}% deles."
    )
    if isinstance(desfechos_tema, pd.DataFrame) and not desfechos_tema.empty:
        linha = desfechos_tema.iloc[0]
        if str(linha["desfecho"]) == CATEGORIA_NAO_CLASSIFICADA:
            insights.append(
                "O app identificou decisões neste tema, mas o texto dos movimentos ainda nao permitiu enquadrar o tipo de desfecho com mais precisão."
            )
        else:
            insights.append(
                f"O desfecho mais frequente para este tema foi `{linha['desfecho']}`, com {format_int_br(linha['quantidade'])} ocorrências."
            )
    else:
        insights.append(
            "Ainda nao foi possivel classificar automaticamente um desfecho predominante para este tema; por isso, vale olhar os movimentos finais mais frequentes."
        )
    if isinstance(movimentos_tema, pd.DataFrame) and not movimentos_tema.empty:
        linha = movimentos_tema.iloc[0]
        insights.append(
            f"O movimento decisório mais comum para este tema foi `{linha['movimento']}`, com {format_int_br(linha['quantidade'])} registros."
        )
    if isinstance(orgaos_tema, pd.DataFrame) and not orgaos_tema.empty:
        linha = orgaos_tema.iloc[0]
        insights.append(
            f"O órgão com mais processos deste tema na amostra foi `{linha['orgao_julgador']}`; nele, o desfecho predominante foi `{linha['desfecho_predominante']}`."
        )
    insights.append(
        f"A robustez estatística desta leitura temática foi classificada como `{forca_tema}`, considerando o volume da amostra e a cobertura de desfechos identificados."
    )
    if favorabilidade_tema.get("indice_favorabilidade") is not None:
        insights.append(
            f"O índice de favorabilidade estimada do tema ficou em `{float(favorabilidade_tema['indice_favorabilidade']):.1f}`, com leitura `{favorabilidade_tema['leitura_favorabilidade']}`."
        )
    if estabilidade_tema.get("indice_estabilidade") is not None:
        insights.append(
            f"O padrão decisório do tema apareceu com estabilidade `{estabilidade_tema['perfil_estabilidade']}`, puxado pelo desfecho líder `{estabilidade_tema['desfecho_lider']}`."
        )
    if concentracao_tema.get("top_orgao"):
        insights.append(
            f"O órgão líder do tema foi `{concentracao_tema['top_orgao']}`, com {concentracao_tema['top_orgao_share']:.1f}% dos processos com órgão identificado; "
            f"os 3 principais órgãos concentram {concentracao_tema['top3_share']:.1f}% da amostra temática com órgão."
        )
    if tendencia_tema.get("ultimo_mes"):
        ultimo_mes = str(tendencia_tema["ultimo_mes"])
        ultimo_valor = format_int_br(tendencia_tema.get("ultimo_valor", 0))
        tendencia = str(tendencia_tema.get("tendencia", "Sem dados"))
        variacao_pct = tendencia_tema.get("variacao_pct")
        if variacao_pct is not None:
            insights.append(
                f"Na série recente do tema, o último mês disponível foi `{ultimo_mes}`, com {ultimo_valor} processos, em um cenário de `{tendencia.lower()}` ({variacao_pct:+.1f}% frente ao mês anterior)."
            )
        else:
            insights.append(
                f"Na série recente do tema, o último mês disponível foi `{ultimo_mes}`, com {ultimo_valor} processos, em um cenário de `{tendencia.lower()}`."
            )
    if int(mudanca_padrao.get("janela_meses", 0) or 0) > 0:
        insights.append(
            f"Na comparação recente do padrão decisório, a leitura foi `{mudanca_padrao['mudanca_principal']}`."
        )
    if alertas_tema:
        insights.append(f"Alertas metodológicos ativos nesta leitura: {len(alertas_tema)}.")
    return insights


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


def assuntos_distintos_dataframe(df_anpp: pd.DataFrame) -> pd.DataFrame:
    if df_anpp.empty or "assuntos" not in df_anpp.columns:
        return pd.DataFrame(columns=["assunto", "quantidade"])

    assuntos = df_anpp["assuntos"].explode().dropna().astype(str).str.strip()
    assuntos = assuntos[assuntos != ""]
    if assuntos.empty:
        return pd.DataFrame(columns=["assunto", "quantidade"])

    return assuntos.value_counts().rename_axis("assunto").reset_index(name="quantidade")


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


def fig_desfechos_tema(desfechos_tema: pd.DataFrame) -> Any:
    plt = get_plt()
    if desfechos_tema.empty:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.set_title("Distribuicao dos desfechos do tema")
        ax.text(0.5, 0.5, "Sem desfechos classificados para este tema.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = desfechos_tema.copy()
    base["desfecho_curto"] = base["desfecho"].astype(str).apply(
        lambda x: x if len(x) <= 32 else x[:32] + "..."
    )
    fig, ax = plt.subplots(figsize=(9, 4.2))
    ax.barh(base["desfecho_curto"], base["quantidade"], color="#E15759", alpha=0.9)
    ax.invert_yaxis()
    ax.set_xlabel("Quantidade")
    ax.set_ylabel("Desfecho")
    ax.set_title("Distribuicao dos desfechos do tema")
    ax.grid(axis="x", linestyle="--", alpha=0.3)

    max_valor = int(pd.to_numeric(base["quantidade"], errors="coerce").fillna(0).max())
    for i, valor in enumerate(base["quantidade"]):
        ax.text(float(valor) + max(max_valor * 0.01, 0.5), i, str(int(valor)), va="center", fontsize=9)

    fig.tight_layout()
    return fig


def fig_desfechos_por_orgao(df_mix: pd.DataFrame) -> Any:
    plt = get_plt()
    if df_mix.empty or "orgao_julgador" not in df_mix.columns:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title("Desfecho por orgao julgador")
        ax.text(0.5, 0.5, "Sem dados suficientes para cruzar orgao e desfecho.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = df_mix.copy()
    colunas_desfecho = [
        coluna for coluna in base.columns if coluna not in {"orgao_julgador", "total_classificados"}
    ]
    if not colunas_desfecho:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title("Desfecho por orgao julgador")
        ax.text(0.5, 0.5, "Sem desfechos classificados para montar o comparativo.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = base[base["total_classificados"] > 0].copy()
    if base.empty:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title("Desfecho por orgao julgador")
        ax.text(0.5, 0.5, "Sem volume classificado suficiente para o comparativo.", ha="center", va="center")
        ax.axis("off")
        return fig

    percentuais = base[colunas_desfecho].div(base["total_classificados"], axis=0) * 100
    labels = [
        orgao if len(orgao) <= 30 else orgao[:30] + "..."
        for orgao in base["orgao_julgador"].astype(str)
    ]
    palette = ["#4E79A7", "#E15759", "#59A14F", "#F28E2B", "#76B7B2", "#EDC948"]
    fig, ax = plt.subplots(figsize=(10.5, max(4.2, len(base) * 0.65 + 1.6)))
    left = pd.Series([0.0] * len(base))

    for idx, coluna in enumerate(colunas_desfecho):
        valores = percentuais[coluna].fillna(0.0)
        ax.barh(
            labels,
            valores,
            left=left,
            label=coluna if len(coluna) <= 28 else coluna[:28] + "...",
            color=palette[idx % len(palette)],
            alpha=0.95,
        )
        left = left + valores

    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xlabel("% dos desfechos classificados")
    ax.set_title("Desfecho por orgao julgador")
    ax.grid(axis="x", linestyle="--", alpha=0.25)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.35), ncol=2, frameon=False)
    fig.tight_layout()
    return fig


def fig_favorabilidade_por_orgao(df_favorabilidade: pd.DataFrame) -> Any:
    plt = get_plt()
    if df_favorabilidade.empty or "orgao_julgador" not in df_favorabilidade.columns:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title("Indice de favorabilidade por orgao")
        ax.text(0.5, 0.5, "Sem base suficiente para medir favorabilidade por orgao.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = df_favorabilidade.copy().sort_values("indice_favorabilidade", ascending=True)
    labels = [
        orgao if len(orgao) <= 28 else orgao[:28] + "..."
        for orgao in base["orgao_julgador"].astype(str)
    ]
    valores = pd.to_numeric(base["indice_favorabilidade"], errors="coerce").fillna(0.0)
    cores = ["#59A14F" if valor >= 0 else "#E15759" for valor in valores]

    fig, ax = plt.subplots(figsize=(10, max(4.2, len(base) * 0.55 + 1.5)))
    ax.barh(labels, valores, color=cores, alpha=0.92)
    ax.axvline(0, color="#BBBBBB", linewidth=1)
    ax.set_xlabel("Indice de favorabilidade estimada")
    ax.set_ylabel("Orgao julgador")
    ax.set_title("Indice de favorabilidade por orgao")
    ax.grid(axis="x", linestyle="--", alpha=0.25)

    for i, valor in enumerate(valores):
        deslocamento = 1.2 if valor >= 0 else -1.2
        alinhamento = "left" if valor >= 0 else "right"
        ax.text(valor + deslocamento, i, f"{valor:.1f}", va="center", ha=alinhamento, fontsize=9)

    fig.tight_layout()
    return fig


def fig_tempo_por_orgao(df_tempo: pd.DataFrame) -> Any:
    plt = get_plt()
    if df_tempo.empty or "orgao_julgador" not in df_tempo.columns:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title("Tempo mediano por orgao")
        ax.text(0.5, 0.5, "Sem base suficiente para comparar tempo por orgao.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = df_tempo.copy().sort_values("mediana_dias", ascending=False)
    labels = [
        orgao if len(orgao) <= 30 else orgao[:30] + "..."
        for orgao in base["orgao_julgador"].astype(str)
    ]
    valores = pd.to_numeric(base["mediana_dias"], errors="coerce").fillna(0.0)

    fig, ax = plt.subplots(figsize=(10, max(4.2, len(base) * 0.55 + 1.5)))
    ax.barh(labels, valores, color="#4E79A7", alpha=0.92)
    ax.set_xlabel("Mediana de dias ate o desfecho")
    ax.set_ylabel("Orgao julgador")
    ax.set_title("Tempo mediano por orgao")
    ax.grid(axis="x", linestyle="--", alpha=0.25)

    for i, valor in enumerate(valores):
        ax.text(valor + max(float(valores.max()) * 0.01, 0.6), i, f"{valor:.1f}", va="center", fontsize=9)

    fig.tight_layout()
    return fig


def fig_tendencia_tema(serie: pd.Series, tema: str) -> Any:
    plt = get_plt()
    titulo = "Evolucao mensal do tema"
    if serie.empty:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
        ax.text(0.5, 0.5, "Sem dados suficientes para a serie mensal deste tema.", ha="center", va="center")
        ax.axis("off")
        return fig

    labels = [idx.strftime("%m/%Y") for idx in serie.index]
    posicoes = list(range(len(serie)))
    valores = pd.to_numeric(serie, errors="coerce").fillna(0.0).tolist()
    tema_curto = tema if len(tema) <= 48 else tema[:48] + "..."

    fig, ax = plt.subplots(figsize=(9.4, 4.2))
    ax.plot(posicoes, valores, color="#59A14F", linewidth=2.4, marker="o", markersize=6)
    ax.fill_between(posicoes, valores, color="#59A14F", alpha=0.12)
    ax.set_title(f"{titulo}: {tema_curto}")
    ax.set_xlabel("Meses")
    ax.set_ylabel("Quantidade")
    ax.set_xticks(posicoes)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    max_valor = max(valores) if valores else 0.0
    for i, valor in enumerate(valores):
        ax.text(i, valor + max(max_valor * 0.02, 0.3), str(int(valor)), ha="center", va="bottom", fontsize=9)

    fig.tight_layout()
    return fig


def save_outputs(df: pd.DataFrame, top_100: pd.Series) -> None:
    plt = get_plt()
    df.to_csv("consulta_datajud.csv", sep=",", header=True, index=False)
    top_orgaos_julgadores_dataframe(df).to_csv("top_orgaos_julgadores_datajud.csv", index=False)

    with open("movimentos_datajud.txt", "w") as file:
        file.write("Arquivo gerado pelo Streamlit.")

    with open("top_100_datajud.txt", "w") as file:
        for index, value in top_100.items():
            file.write(f"{index[0]} | {index[1]} | {value}\n")

    fig1 = fig_horario(df)
    fig1.savefig("horario_datajud.jpg")
    plt.close(fig1)

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
    st.markdown(
        """
        <style>
        .theme-metric-card {
            min-height: 8.7rem;
            padding: 0.25rem 0 0.1rem 0;
        }
        .theme-metric-label {
            font-size: 0.98rem;
            font-weight: 600;
            line-height: 1.25;
            color: rgba(250, 250, 250, 0.92);
            margin-bottom: 0.45rem;
        }
        .theme-metric-value {
            font-size: clamp(1.7rem, 2vw, 3rem);
            font-weight: 700;
            line-height: 1.02;
            color: rgba(250, 250, 250, 0.98);
            white-space: normal;
            overflow-wrap: anywhere;
            word-break: break-word;
        }
        .theme-metric-delta {
            display: inline-block;
            margin-top: 0.6rem;
            padding: 0.18rem 0.55rem;
            border-radius: 999px;
            background: rgba(41, 122, 74, 0.35);
            color: #9EE6AE;
            font-size: 0.9rem;
            line-height: 1.1;
            font-weight: 600;
            white-space: normal;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("Jurimetria com a API DataJud")
    st.markdown(
        "Por **Lucas Martins** | Bibliotecario e Advogado | CRB6-3621 | OAB/MG 243736  \n"
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
        estrutura_info = get_estrutura_options(tribunal_sigla)
        estrutura_filtro = st.selectbox(
            "Instancia / estrutura do tribunal (opcional)",
            options=estrutura_info["opcoes"],
            index=0,
            format_func=format_estrutura_option,
            help="Use para separar a analise por grau, juizado, turma recursal ou estrutura equivalente.",
        )
        st.caption(describe_estrutura_option(estrutura_filtro))
        st.caption(str(estrutura_info["observacao"]))
        classe_codigo = st.number_input(
            "Classe codigo",
            min_value=1,
            value=12729,
            step=1,
        )
        render_codigo_sugestoes(tribunal_sigla)
        st.markdown(
            f"[Consultar codigos de classe (CNJ)]({CNJ_CLASSES_URL})"
        )
        numero_processo = st.text_input(
            "Numero do processo (opcional)",
            placeholder="Ex.: 50012345620248130024",
            help="Se preenchido, a consulta usa o numero do processo em vez da classe.",
        )
        aplicar_periodo = st.checkbox(
            "Filtrar por periodo de ajuizamento",
            value=False,
            help="Limita a amostra a um intervalo de datas de ajuizamento.",
        )
        data_inicio = None
        data_fim = None
        if aplicar_periodo:
            hoje = date.today()
            inicio_padrao = date(hoje.year, 1, 1)
            col_data_inicio, col_data_fim = st.columns(2)
            with col_data_inicio:
                data_inicio = st.date_input(
                    "Data inicial",
                    value=inicio_padrao,
                )
            with col_data_fim:
                data_fim = st.date_input(
                    "Data final",
                    value=hoje,
                )
            periodo_legivel = format_periodo_aplicado(data_inicio, data_fim)
            if periodo_legivel:
                st.caption(f"Periodo aplicado: {periodo_legivel}")
        st.caption(
            "Ao executar a consulta, o app tambem monta automaticamente um mapa da sigla com os codigos, classes e assuntos mais comuns."
        )
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
        st.session_state["avisos_consulta"] = []
        if not api_key:
            st.error("API Key ausente. Configure DATAJUD_API_KEY no servidor.")
            return
        if aplicar_periodo:
            inicio_validado = coerce_date_value(data_inicio)
            fim_validado = coerce_date_value(data_fim)
            if inicio_validado and fim_validado and inicio_validado > fim_validado:
                st.error("A data inicial nao pode ser maior que a data final.")
                return

        with st.spinner("Buscando dados no DataJud..."):
            started = time.perf_counter()
            avisos_consulta: list[str] = []
            try:
                usar_numero_processo = bool(normalize_numero_processo(numero_processo))
                data_inicio_consulta = None if usar_numero_processo else data_inicio
                data_fim_consulta = None if usar_numero_processo else data_fim
                hits = fetch_hits(
                    api_key=api_key,
                    classe_codigo=int(classe_codigo),
                    size=int(size),
                    url=url,
                    numero_processo=numero_processo,
                    data_inicio=data_inicio_consulta,
                    data_fim=data_fim_consulta,
                    incluir_movimentos=not modo_rapido,
                    modo_consulta="classe_ou_processo",
                )
                df_anpp = hits_to_dataframe(hits, processar_movimentos=not modo_rapido)
                if not usar_numero_processo:
                    df_anpp = filter_dataframe_by_estrutura(df_anpp, tribunal_sigla, estrutura_filtro)
                else:
                    df_anpp = add_estrutura_column(df_anpp, tribunal_sigla)
                top_100 = build_top_100(df_anpp)
                mapa_size = min(max(int(size), 2000), MAX_PAGE_SIZE)
                decisao_size = min(max(int(size), 400), 1200)
                top_codigos = pd.DataFrame()
                top_classes = pd.DataFrame()
                top_assuntos = pd.DataFrame()
                df_decisao = pd.DataFrame()
                qtd_mapa = 0
                qtd_decisao = 0

                if not usar_numero_processo:
                    if modo_rapido:
                        try:
                            hits_decisao = fetch_hits(
                                api_key=api_key,
                                classe_codigo=int(classe_codigo),
                                size=decisao_size,
                                url=url,
                                numero_processo="",
                                data_inicio=data_inicio_consulta,
                                data_fim=data_fim_consulta,
                                incluir_movimentos=True,
                                modo_consulta="classe_ou_processo",
                            )
                            df_decisao = hits_to_dataframe(hits_decisao, processar_movimentos=True)
                        except DataJudRequestError as exc:
                            avisos_consulta.append(
                                "Nao consegui montar a leitura decisoria complementar nesta tentativa. "
                                f"{exc}"
                            )
                            df_decisao = pd.DataFrame()
                    else:
                        df_decisao = df_anpp.head(decisao_size).copy()

                    if not df_decisao.empty:
                        df_decisao = enrich_decision_proxy_dataframe(df_decisao)
                        df_decisao = filter_dataframe_by_estrutura(df_decisao, tribunal_sigla, estrutura_filtro)
                        qtd_decisao = len(df_decisao)

                    try:
                        hits_mapa = fetch_hits(
                            api_key=api_key,
                            classe_codigo=int(classe_codigo),
                            size=mapa_size,
                            url=url,
                            numero_processo="",
                            data_inicio=data_inicio_consulta,
                            data_fim=data_fim_consulta,
                            incluir_movimentos=False,
                            modo_consulta="mapa_tribunal",
                        )
                        df_mapa = hits_to_dataframe(hits_mapa, processar_movimentos=False)
                        df_mapa = filter_dataframe_by_estrutura(df_mapa, tribunal_sigla, estrutura_filtro)
                        top_codigos = top_codigos_dataframe(df_mapa)
                        top_classes = top_classes_dataframe(df_mapa)
                        top_assuntos = top_assuntos_dataframe(df_mapa)
                        qtd_mapa = len(df_mapa)
                    except DataJudRequestError as exc:
                        avisos_consulta.append(
                            "Nao consegui montar o mapa automatico da sigla nesta tentativa. "
                            f"{exc}"
                        )

                # Se a amostra vier curta para histórico mensal, tenta ampliar só para o gráfico.
                df_mensal = df_anpp
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
                                data_inicio=data_inicio_consulta,
                                data_fim=data_fim_consulta,
                                incluir_movimentos=False,
                                modo_consulta="classe_ou_processo",
                            )
                            df_mensal_candidato = hits_to_dataframe(hits_mensal, processar_movimentos=False)
                            df_mensal_candidato = filter_dataframe_by_estrutura(
                                df_mensal_candidato,
                                tribunal_sigla,
                                estrutura_filtro,
                            )
                            meses_candidato = len(monthly_counts(df_mensal_candidato, max_meses=12))
                            if meses_candidato > meses_base:
                                df_mensal = df_mensal_candidato
                                st.info(
                                    "Para o gráfico mensal, usei amostra ampliada (10.000 registros)."
                                )
                        except DataJudRequestError as exc:
                            avisos_consulta.append(
                                "Nao consegui ampliar o historico mensal nesta tentativa. "
                                f"{exc}"
                            )
                        except Exception:
                            pass
            except DataJudRequestError as exc:
                st.error(str(exc))
                return
            except HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                st.error(build_datajud_error_message(status, size=int(size), numero_processo=numero_processo))
                return
            except RequestException:
                st.error(
                    "Nao foi possivel se comunicar com o DataJud nesta tentativa. "
                    "Tente novamente em alguns minutos."
                )
                return
            except Exception as exc:
                st.exception(exc)
                return
        elapsed = time.perf_counter() - started

        st.session_state["df_anpp"] = df_anpp
        st.session_state["df_mensal"] = df_mensal
        st.session_state["top_100"] = top_100
        st.session_state["hits"] = hits
        st.session_state["sigla_mapa"] = tribunal_sigla
        st.session_state["qtd_mapa"] = qtd_mapa
        st.session_state["top_codigos"] = top_codigos
        st.session_state["top_classes"] = top_classes
        st.session_state["top_assuntos"] = top_assuntos
        st.session_state["df_decisao"] = df_decisao
        st.session_state["qtd_decisao"] = qtd_decisao
        st.session_state["usar_numero_processo"] = usar_numero_processo
        st.session_state["estrutura_filtro"] = estrutura_filtro
        st.session_state["periodo_aplicado"] = format_periodo_aplicado(data_inicio_consulta, data_fim_consulta)
        st.session_state["periodo_ignorado_numero"] = bool(usar_numero_processo and aplicar_periodo)
        st.session_state["avisos_consulta"] = avisos_consulta
        st.success(f"Consulta concluida em {elapsed:.1f}s. Registros: {len(df_anpp)}")

    if "df_anpp" not in st.session_state:
        st.info("Preencha a chave e clique em 'Executar consulta'. Comece com 1000 ou 2000 registros.")
        return

    df_anpp = st.session_state["df_anpp"]
    df_mensal = st.session_state.get("df_mensal", df_anpp)
    top_100 = st.session_state["top_100"]
    top_codigos = st.session_state.get("top_codigos", pd.DataFrame())
    top_classes = st.session_state.get("top_classes", pd.DataFrame())
    top_assuntos = st.session_state.get("top_assuntos", pd.DataFrame())
    df_decisao = st.session_state.get("df_decisao", pd.DataFrame())
    qtd_mapa = int(st.session_state.get("qtd_mapa", 0) or 0)
    qtd_decisao = int(st.session_state.get("qtd_decisao", 0) or 0)
    usar_numero_processo = bool(st.session_state.get("usar_numero_processo", False))
    estrutura_filtro = str(st.session_state.get("estrutura_filtro", "Todos"))
    periodo_aplicado = str(st.session_state.get("periodo_aplicado", ""))
    periodo_ignorado_numero = bool(st.session_state.get("periodo_ignorado_numero", False))
    avisos_consulta = st.session_state.get("avisos_consulta", [])
    df_view = dataframe_for_display(df_anpp, max_rows=400)
    top_100_df = top_100_to_dataframe(top_100)
    top_orgaos_df = top_orgaos_julgadores_dataframe(df_anpp)
    sample_insights = build_sample_insights(df_anpp, df_mensal, top_orgaos_df, top_100_df)
    map_insights = build_map_insights(top_codigos, top_classes, top_assuntos, qtd_mapa)
    tema_insights: list[str] = []
    tema_escolhido = ""
    assuntos_distintos = assuntos_distintos_dataframe(df_anpp)
    total_assuntos = (
        df_anpp["assuntos"].explode().dropna().astype(str).nunique()
        if "assuntos" in df_anpp.columns
        else 0
    )

    st.subheader("Resumo")
    c1, c2, c3 = st.columns(3)
    c1.metric("Registros", f"{len(df_anpp):,}".replace(",", "."))
    c2.metric("Temas diferentes", str(total_assuntos))
    c3.metric("Orgaos julgadores", str(df_anpp["orgao_julgador"].nunique()))

    if estrutura_filtro != "Todos" and not usar_numero_processo:
        st.caption(
            f"Filtro estrutural aplicado: {format_estrutura_option(estrutura_filtro)}. "
            "Quando a API nao traz a estrutura de forma explicita, o app estima pelo grau e pelo nome do orgao julgador."
        )
    if periodo_aplicado:
        st.caption(f"Filtro temporal aplicado no ajuizamento: {periodo_aplicado}.")
    elif periodo_ignorado_numero:
        st.caption(
            "O filtro temporal foi ignorado porque a consulta por numero do processo prioriza o caso exato."
        )
    for aviso in avisos_consulta:
        st.warning(aviso)

    if not assuntos_distintos.empty:
        with st.expander("Ver temas diferentes desta amostra", expanded=False):
            st.caption("Esta lista mostra os assuntos distintos encontrados na amostra atual da consulta, com a quantidade de ocorrencias.")
            st.dataframe(assuntos_distintos, use_container_width=True, height=320)

    if usar_numero_processo:
        st.info(
            "A leitura decisoria por tema aparece nas consultas por classe/tema. "
            "Quando voce busca por numero do processo, o app mostra o caso individual e nao aplica o filtro estrutural para nao esconder o processo."
        )
    elif isinstance(df_decisao, pd.DataFrame) and not df_decisao.empty:
        temas_decisao = assuntos_distintos_dataframe(df_decisao)
        temas_overview = theme_overview_dataframe(df_decisao)
        st.subheader("Leitura decisoria por tema")
        st.caption(
            "Esta leitura usa o ultimo movimento decisorio identificado em cada processo como proxy do desfecho. "
            "Quando o retorno nao expoe juiz ou relator, a comparacao e feita por orgao julgador."
        )
        if qtd_decisao:
            st.caption(
                f"Analise baseada em ate {qtd_decisao:,} registros recentes com movimentos completos.".replace(",", ".")
            )

        tema_opcoes = temas_decisao["assunto"].tolist()
        if tema_opcoes:
            tema_escolhido = st.selectbox(
                "Tema para analisar",
                options=["Todos os temas"] + tema_opcoes,
                index=0,
                help="Escolha um tema especifico ou volte para a visao geral de todos os temas.",
            )
        else:
            tema_escolhido = ""

        if tema_escolhido == "Todos os temas":
            total_temas_mapeados = int(len(temas_decisao))
            d1, d2, d3 = st.columns(3)
            d1.metric("Temas mapeados", format_int_br(total_temas_mapeados))
            d2.metric("Temas na visao geral", format_int_br(len(temas_overview)))
            d3.metric("Base com movimentos", f"{qtd_decisao:,}".replace(",", "."))
            st.caption(
                "Selecione um tema especifico no filtro acima para ver desfechos, movimentos finais, orgaos e contexto daquele tema."
            )
            st.markdown("**Visao geral dos temas da amostra**")
            st.dataframe(temas_overview, use_container_width=True, height=360)
        elif tema_escolhido:
            df_tema_decisao = filter_dataframe_by_tema(df_decisao, tema_escolhido)
            desfechos_tema = decision_outcomes_dataframe(df_tema_decisao)
            movimentos_tema = decision_movements_dataframe(df_tema_decisao)
            orgaos_tema = decision_by_orgao_dataframe(df_tema_decisao)
            mix_orgaos_tema = decision_outcome_mix_by_orgao_dataframe(df_tema_decisao)
            classes_tema = top_classes_dataframe(df_tema_decisao)
            temas_relacionados = related_themes_dataframe(df_tema_decisao, tema_escolhido)
            cobertura_tema = decision_coverage_summary(df_tema_decisao)
            forca_tema = theme_sample_strength_label(
                int(cobertura_tema["total_processos"]),
                int(cobertura_tema["com_desfecho"]),
            )
            concentracao_tema = theme_concentration_summary(df_tema_decisao)
            tendencia_tema = theme_recent_trend_summary(df_tema_decisao)
            favorabilidade_tema = decision_favorability_summary(df_tema_decisao)
            estabilidade_tema = decision_stability_summary(df_tema_decisao)
            favorabilidade_orgaos = decision_favorability_by_orgao_dataframe(
                df_tema_decisao,
                min_decisoes_uteis=5,
                max_items=None,
            )
            tempo_orgaos = decision_time_by_orgao_dataframe(
                df_tema_decisao,
                min_processos=3,
                max_items=None,
            )
            estabilidade_orgaos = decision_stability_by_orgao_dataframe(
                df_tema_decisao,
                min_classificados=5,
                max_items=None,
            )
            mudanca_padrao = decision_pattern_change_summary(df_tema_decisao)
            total_tema = int(cobertura_tema["total_processos"])
            total_com_desfecho = int(cobertura_tema["com_desfecho"])
            total_com_movimento = int(cobertura_tema["com_movimento_final"])
            alertas_tema = theme_sample_alerts(
                total_tema,
                total_com_desfecho,
                favorabilidade_tema,
                favorabilidade_orgaos,
                tempo_orgaos,
                mudanca_padrao,
            )
            cobertura = float(cobertura_tema["cobertura_desfecho"])
            cobertura_movimento = float(cobertura_tema["cobertura_movimento"])
            desfecho_predominante = (
                str(desfechos_tema.iloc[0]["desfecho"])
                if not desfechos_tema.empty
                else "Sem classificacao automatica"
            )
            desfecho_predominante_card = (
                "Nao classificado"
                if desfecho_predominante == CATEGORIA_NAO_CLASSIFICADA
                else desfecho_predominante
            )
            dias_decisao = pd.to_numeric(
                df_tema_decisao["dias_ate_decisao_proxy"], errors="coerce"
            ).dropna()
            mediana_dias = f"{dias_decisao.median():.0f} dias" if not dias_decisao.empty else "-"
            top_orgao_tema = str(concentracao_tema.get("top_orgao", "")).strip()
            top_orgao_tema_curto = (
                top_orgao_tema if len(top_orgao_tema) <= 42 else top_orgao_tema[:42] + "..."
            )
            top_orgao_share = float(concentracao_tema.get("top_orgao_share", 0.0) or 0.0)
            top3_share = float(concentracao_tema.get("top3_share", 0.0) or 0.0)
            total_com_orgao = int(concentracao_tema.get("total_com_orgao", 0) or 0)
            tendencia_label = str(tendencia_tema.get("tendencia", "Sem dados"))
            tendencia_delta = (
                f"{float(tendencia_tema['variacao_pct']):+.1f}% vs mes anterior"
                if tendencia_tema.get("variacao_pct") is not None
                else None
            )
            serie_tema = tendencia_tema.get("serie", pd.Series(dtype="int64"))
            indice_favorabilidade = favorabilidade_tema.get("indice_favorabilidade")
            leitura_favorabilidade = str(favorabilidade_tema.get("leitura_favorabilidade", "Sem base"))
            delta_favorabilidade = (
                f"{float(indice_favorabilidade):+.1f}"
                if indice_favorabilidade is not None
                else None
            )
            indice_estabilidade = estabilidade_tema.get("indice_estabilidade")
            perfil_estabilidade = str(estabilidade_tema.get("perfil_estabilidade", "Sem base"))
            delta_estabilidade = (
                f"{float(indice_estabilidade):.1f}"
                if indice_estabilidade is not None
                else None
            )
            mudanca_label = str(mudanca_padrao.get("mudanca_principal", "Sem base"))
            delta_mudanca = (
                f"{float(mudanca_padrao['delta_indice']):+.1f}"
                if mudanca_padrao.get("delta_indice") is not None
                else None
            )
            ranking_favoraveis = (
                favorabilidade_orgaos.sort_values(
                    ["indice_favorabilidade", "decisoes_uteis"], ascending=[False, False]
                )
                .head(5)
                .reset_index(drop=True)
                if not favorabilidade_orgaos.empty
                else pd.DataFrame()
            )
            ranking_restritivos = (
                favorabilidade_orgaos.sort_values(
                    ["indice_favorabilidade", "decisoes_uteis"], ascending=[True, False]
                )
                .head(5)
                .reset_index(drop=True)
                if not favorabilidade_orgaos.empty
                else pd.DataFrame()
            )

            d1, d2, d3, d4 = st.columns(4)
            render_theme_metric_card(d1, "Processos do tema", f"{total_tema:,}".replace(",", "."))
            render_theme_metric_card(d2, "Desfecho classificado", f"{cobertura:.1f}%")
            render_theme_metric_card(d3, "Desfecho predominante", desfecho_predominante_card)
            render_theme_metric_card(d4, "Movimento final identificado", f"{cobertura_movimento:.1f}%")
            e1, e2, e3, e4 = st.columns(4)
            render_theme_metric_card(e1, "Robustez da amostra", forca_tema)
            render_theme_metric_card(e2, "Favorabilidade estimada", leitura_favorabilidade, delta_favorabilidade)
            render_theme_metric_card(e3, "Estabilidade decisoria", perfil_estabilidade, delta_estabilidade)
            render_theme_metric_card(e4, "Mudanca recente", mudanca_label, delta_mudanca)
            st.caption(
                "Legenda: desfecho predominante = sinal mais comum; robustez = forca da base; "
                "favorabilidade = tendencia mais pro ou contra; estabilidade = repeticao do padrao; "
                "mudanca recente = se esse comportamento mudou nos ultimos meses."
            )
            tema_insights = build_decision_theme_insights(
                tema_escolhido,
                total_tema,
                total_com_desfecho,
                desfechos_tema,
                movimentos_tema,
                orgaos_tema,
                forca_tema,
                concentracao_tema,
                tendencia_tema,
                favorabilidade_tema,
                estabilidade_tema,
                mudanca_padrao,
                alertas_tema,
            )
            tema_tabs = st.tabs(["Resumo do tema", "Leituras", "Orgaos", "Estrategia", "Contexto do tema"])
            with tema_tabs[0]:
                st.caption(
                    "Aqui o app resume o tema escolhido com base nos processos da amostra e nos movimentos mais recentes encontrados."
                )
                if forca_tema == "Baixa":
                    st.info(
                        "A amostra deste tema ainda e pequena ou tem pouca cobertura de desfechos. Use a leitura como sinal inicial, nao como padrao fechado."
                    )
                elif forca_tema == "Media":
                    st.caption(
                        "A leitura deste tema ja ajuda na estrategia, mas ainda vale conferir o contexto dos orgaos e dos movimentos finais."
                    )
                col_r1, col_r2, col_r3 = st.columns(3)
                with col_r1:
                    st.markdown("**Cobertura da leitura**")
                    st.markdown(
                        f"- Processos com algum movimento final identificado: {format_int_br(total_com_movimento)} ({cobertura_movimento:.1f}%)"
                    )
                    st.markdown(
                        f"- Processos com desfecho classificado automaticamente: {format_int_br(total_com_desfecho)} ({cobertura:.1f}%)"
                    )
                    st.markdown(f"- Mediana ate o desfecho identificado: {mediana_dias}")
                with col_r2:
                    st.markdown("**Sinal principal do tema**")
                    st.markdown(f"- Desfecho predominante: {desfecho_predominante_card}")
                    if not movimentos_tema.empty:
                        st.markdown(
                            f"- Movimento final mais frequente: {movimentos_tema.iloc[0]['movimento']}"
                        )
                    else:
                        st.markdown("- Movimento final mais frequente: sem leitura")
                with col_r3:
                    st.markdown("**Sinais estrategicos**")
                    st.markdown(f"- Robustez estatistica da leitura: {forca_tema}")
                    st.markdown(f"- Favorabilidade estimada: {leitura_favorabilidade}")
                    st.markdown(f"- Estabilidade decisoria: {perfil_estabilidade}")
                    if top_orgao_tema:
                        st.markdown(
                            f"- Orgao lider do tema: {top_orgao_tema_curto} ({top_orgao_share:.1f}% dos processos com orgao identificado)"
                        )
                    else:
                        st.markdown("- Orgao lider do tema: sem identificacao suficiente")
                    if total_com_orgao:
                        st.markdown(
                            f"- Top 3 orgaos na amostra: {top3_share:.1f}% dos {format_int_br(total_com_orgao)} processos com orgao identificado"
                        )
                    else:
                        st.markdown("- Top 3 orgaos na amostra: sem base suficiente")
                st.markdown("**Evolucao mensal deste tema**")
                st.caption("Mostra se o volume recente do tema sobe, cai ou permanece estavel nos meses disponiveis da amostra.")
                col_trend_chart, col_trend_notes = st.columns([1.4, 1.0])
                with col_trend_chart:
                    st.pyplot(fig_tendencia_tema(serie_tema, tema_escolhido), clear_figure=True)
                with col_trend_notes:
                    st.markdown("**Leitura da tendencia**")
                    if tendencia_tema.get("ultimo_mes"):
                        st.markdown(
                            f"- Ultimo mes disponivel: {tendencia_tema['ultimo_mes']} com {format_int_br(tendencia_tema['ultimo_valor'])} processos"
                        )
                    else:
                        st.markdown("- Ultimo mes disponivel: sem base suficiente")
                    st.markdown(f"- Sinal recente: {tendencia_label}")
                    if tendencia_tema.get("variacao_pct") is not None:
                        st.markdown(
                            f"- Variacao frente ao mes anterior: {float(tendencia_tema['variacao_pct']):+.1f}%"
                        )
                    else:
                        st.markdown("- Variacao frente ao mes anterior: sem comparacao disponivel")
                    st.markdown(
                        f"- Janela observada: {format_int_br(tendencia_tema.get('meses_base', 0))} meses"
                    )
                    if int(mudanca_padrao.get("janela_meses", 0) or 0) > 0:
                        st.markdown(f"- Mudanca recente do padrao: {mudanca_label}")
            with tema_tabs[1]:
                col_desfechos, col_movimentos = st.columns(2)
                with col_desfechos:
                    st.markdown("**Desfechos classificados no tema**")
                    st.caption("Mostra como os desfechos classificados se distribuem neste tema.")
                    if not desfechos_tema.empty:
                        if bool((desfechos_tema["desfecho"] == CATEGORIA_NAO_CLASSIFICADA).any()):
                            st.caption(
                                "Quando aparecer 'Decisao identificada, mas nao classificada', houve sinal decisorio, "
                                "mas o texto do movimento nao permitiu definir um tipo mais especifico."
                            )
                        st.pyplot(fig_desfechos_tema(desfechos_tema), clear_figure=True)
                        st.dataframe(desfechos_tema, use_container_width=True, height=300)
                    else:
                        st.info(
                            "Ainda nao foi possivel classificar desfechos automaticamente para este tema nesta amostra. "
                            "Use a tabela de movimentos finais ao lado como apoio."
                        )
                with col_movimentos:
                    st.markdown("**Movimentos finais mais frequentes**")
                    st.caption("Mostra o movimento final mais recorrente, mesmo quando nao ha desfecho classificado.")
                    if not movimentos_tema.empty:
                        st.dataframe(movimentos_tema, use_container_width=True, height=300)
                    else:
                        st.info("Nao encontrei movimentos finais suficientes para este tema.")
            with tema_tabs[2]:
                st.markdown("**Como os orgaos julgadores aparecem neste tema**")
                st.caption("Compara volume, cobertura e sinal principal por orgao julgador neste tema.")
                if not mix_orgaos_tema.empty:
                    st.markdown("**Desfecho por orgao julgador**")
                    st.caption("Compara a composicao dos desfechos classificados entre os principais orgaos do tema.")
                    col_mix_chart, col_mix_table = st.columns(2)
                    with col_mix_chart:
                        st.pyplot(fig_desfechos_por_orgao(mix_orgaos_tema), clear_figure=True)
                    with col_mix_table:
                        st.dataframe(mix_orgaos_tema, use_container_width=True, height=360)
                else:
                    st.info(
                        "Ainda nao ha desfechos classificados suficientes para comparar os orgaos julgadores neste tema."
                    )
                st.markdown("**Resumo por orgao julgador**")
                if not orgaos_tema.empty:
                    st.dataframe(orgaos_tema, use_container_width=True, height=320)
                else:
                    st.info("Nao encontrei dados suficientes por orgao julgador para este tema.")
                st.markdown("**Taxa de desfecho por orgao**")
                st.caption(
                    "Mostra, por orgao, a proporcao estimada de sinais favoraveis, desfavoraveis e mistos entre as decisoes uteis do tema."
                )
                if not favorabilidade_orgaos.empty:
                    st.dataframe(
                        favorabilidade_orgaos.head(12),
                        use_container_width=True,
                        height=320,
                    )
                else:
                    st.info(
                        "Ainda nao ha base util suficiente para medir favorabilidade estimada por orgao neste tema."
                    )
            with tema_tabs[3]:
                st.caption(
                    "Estas metricas ajudam na estrategia, mas funcionam como proxy automatica. Use junto da leitura juridica do tema e do orgao."
                )
                col_estrat1, col_estrat2 = st.columns([1.15, 0.85])
                with col_estrat1:
                    st.markdown("**Indice de favorabilidade por orgao**")
                    st.caption("Compara quais orgaos parecem mais receptivos ou mais restritivos para o tema, com base nas decisoes classificadas.")
                    st.pyplot(
                        fig_favorabilidade_por_orgao(favorabilidade_orgaos.head(10)),
                        clear_figure=True,
                    )
                with col_estrat2:
                    st.markdown("**Favorabilidade do tema**")
                    if int(favorabilidade_tema.get("decisoes_uteis", 0) or 0) > 0:
                        st.markdown(
                            f"- Favoravel estimado: {favorabilidade_tema['favoravel_pct']:.1f}%"
                        )
                        st.markdown(
                            f"- Desfavoravel estimado: {favorabilidade_tema['desfavoravel_pct']:.1f}%"
                        )
                        st.markdown(
                            f"- Misto/parcial: {favorabilidade_tema['misto_pct']:.1f}%"
                        )
                        st.markdown(
                            f"- Neutro/processual fora do indice: {favorabilidade_tema['neutro_pct']:.1f}% dos classificados"
                        )
                    else:
                        st.info("Ainda nao ha massa critica de desfechos uteis para medir favorabilidade do tema.")
                    st.markdown("**Mudanca recente do padrao**")
                    if int(mudanca_padrao.get("janela_meses", 0) or 0) > 0:
                        st.markdown(f"- Leitura principal: {mudanca_label}")
                        st.markdown(
                            f"- Janela recente: {', '.join(mudanca_padrao.get('meses_recentes', [])) or 'sem base'}"
                        )
                        st.markdown(
                            f"- Janela anterior: {', '.join(mudanca_padrao.get('meses_anteriores', [])) or 'sem base'}"
                        )
                        if mudanca_padrao.get("desfecho_lider_recente"):
                            st.markdown(
                                f"- Desfecho lider recente: {mudanca_padrao['desfecho_lider_recente']}"
                            )
                        if mudanca_padrao.get("desfecho_lider_anterior"):
                            st.markdown(
                                f"- Desfecho lider anterior: {mudanca_padrao['desfecho_lider_anterior']}"
                            )
                        if mudanca_padrao.get("delta_indice") is not None:
                            st.markdown(
                                f"- Variacao do indice de favorabilidade: {float(mudanca_padrao['delta_indice']):+.1f}"
                            )
                    else:
                        st.info("Ainda nao ha meses suficientes com decisao classificada para medir mudanca recente do padrao.")
                    st.markdown("**Alertas de leitura**")
                    if alertas_tema:
                        for alerta in alertas_tema:
                            st.markdown(f"- {alerta}")
                    else:
                        st.success("A leitura deste tema nao ativou alertas metodologicos importantes.")
                col_rank1, col_rank2 = st.columns(2)
                with col_rank1:
                    st.markdown("**Orgaos mais favoraveis**")
                    st.caption("Ranking com base no indice de favorabilidade estimada, considerando apenas orgaos com base util minima.")
                    if not ranking_favoraveis.empty:
                        st.dataframe(ranking_favoraveis, use_container_width=True, height=260)
                    else:
                        st.info("Sem base suficiente para ranquear orgaos mais favoraveis neste tema.")
                with col_rank2:
                    st.markdown("**Orgaos mais restritivos**")
                    st.caption("Mostra os orgaos cujo sinal estimado foi mais desfavoravel no tema, respeitando base minima.")
                    if not ranking_restritivos.empty:
                        st.dataframe(ranking_restritivos, use_container_width=True, height=260)
                    else:
                        st.info("Sem base suficiente para ranquear orgaos mais restritivos neste tema.")
                col_tempo_chart, col_tempo_table = st.columns(2)
                with col_tempo_chart:
                    st.markdown("**Tempo mediano ate o desfecho por orgao**")
                    st.caption("Mostra quais orgaos tendem a decidir mais rapido ou mais devagar dentro do tema.")
                    st.pyplot(fig_tempo_por_orgao(tempo_orgaos.head(10)), clear_figure=True)
                    if not tempo_orgaos.empty:
                        st.dataframe(tempo_orgaos.head(12), use_container_width=True, height=260)
                    else:
                        st.info("Sem base suficiente para comparar tempo mediano por orgao.")
                with col_tempo_table:
                    st.markdown("**Estabilidade decisoria por orgao**")
                    st.caption("Mostra se cada orgao repete mais o mesmo desfecho ou oscila entre sinais diferentes.")
                    if not estabilidade_orgaos.empty:
                        st.dataframe(estabilidade_orgaos.head(12), use_container_width=True, height=320)
                    else:
                        st.info("Sem base suficiente para medir estabilidade decisoria por orgao.")
            with tema_tabs[4]:
                col_classes, col_relacionados = st.columns(2)
                with col_classes:
                    st.markdown("**Classes mais frequentes neste tema**")
                    st.caption("Mostra em quais classes processuais este tema mais aparece.")
                    if not classes_tema.empty:
                        st.dataframe(classes_tema, use_container_width=True, height=300)
                    else:
                        st.info("Sem classes suficientes para este tema.")
                with col_relacionados:
                    st.markdown("**Temas relacionados na mesma amostra**")
                    st.caption("Mostra outros temas que costumam aparecer junto deste na mesma amostra.")
                    if not temas_relacionados.empty:
                        st.dataframe(temas_relacionados, use_container_width=True, height=300)
                    else:
                        st.info("Nao encontrei outros temas recorrentes junto com este.")
        else:
            st.info("Nao encontrei temas suficientes para montar a leitura decisoria.")

    st.subheader("Tabela")
    st.caption("Tabela simplificada (amostra de ate 400 linhas) para evitar travamento.")
    st.dataframe(df_view, use_container_width=True, height=350)

    st.subheader("Top 100 por municipio e orgao julgador")
    st.caption("Lista as combinacoes de municipio e orgao julgador que mais aparecem na amostra.")
    st.dataframe(top_100_df, use_container_width=True, height=350)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Horario")
        st.caption("Mostra em quais horas houve mais ajuizamentos na amostra consultada.")
        st.pyplot(fig_horario(df_anpp), clear_figure=True)
    with col_b:
        st.subheader("Top 10 orgaos julgadores")
        st.caption("Mostra os orgaos que mais aparecem na amostra atual, com a participacao de cada um no total consultado.")
        st.dataframe(top_orgaos_df, use_container_width=True, height=320)

    st.subheader("Ajuizamentos mensais")
    st.caption("Mostra a evolucao mensal dos ajuizamentos dentro da amostra consultada.")
    st.pyplot(fig_mensal(df_mensal), clear_figure=True)

    if mostrar_graficos_avancados:
        st.subheader("Fluxo mensal")
        st.caption("Compara ajuizados e atualizados por mes; atualizados usa 'ultima_atualizacao' como proxy de andamento/saida.")
        st.pyplot(fig_fluxo_mensal(df_mensal), clear_figure=True)

        st.subheader("Tempo de tramitacao por orgao")
        st.caption("Compara a distribuicao do tempo entre ajuizamento e ultima atualizacao nos principais orgaos.")
        st.pyplot(fig_tempo_tramitacao_boxplot(df_anpp), clear_figure=True)

        st.subheader("Heatmap dia x hora")
        st.caption("Mostra em que dias da semana e horarios a amostra se concentra.")
        st.pyplot(fig_heatmap_dia_hora(df_anpp), clear_figure=True)
    else:
        st.caption("Graficos avancados ocultos para resposta mais rapida. Ative na barra lateral.")

    if isinstance(top_codigos, pd.DataFrame) and not top_codigos.empty:
        sigla_mapa = str(st.session_state.get("sigla_mapa", "")).strip().upper()
        titulo_mapa = "Mapa automatico da sigla do tribunal"
        if sigla_mapa:
            titulo_mapa = f"Mapa automatico da sigla do tribunal ({sigla_mapa})"
        st.subheader(titulo_mapa)
        if qtd_mapa:
            mensagem_mapa = (
                f"Os rankings abaixo se referem a sigla do tribunal selecionado e usam uma amostra automatica de ate {qtd_mapa:,} registros recentes.".replace(",", ".")
            )
            if estrutura_filtro != "Todos" and not usar_numero_processo:
                mensagem_mapa += f" Filtro estrutural aplicado: {format_estrutura_option(estrutura_filtro)}."
            if periodo_aplicado:
                mensagem_mapa += f" Periodo de ajuizamento: {periodo_aplicado}."
            st.caption(mensagem_mapa)
        else:
            st.caption("Os rankings abaixo se referem a sigla do tribunal selecionado.")
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

    st.subheader("Resumos automaticos")
    st.caption(
        "Leituras em linguagem simples geradas a partir da amostra atual. Elas ajudam na interpretacao inicial, "
        "mas nao substituem a leitura juridica do caso concreto."
    )
    abas_resumo = ["Amostra atual"]
    if map_insights:
        abas_resumo.append("Mapa da sigla")
    if tema_insights:
        abas_resumo.append("Tema selecionado")
    resumo_tabs = st.tabs(abas_resumo)
    aba_idx = 0
    with resumo_tabs[aba_idx]:
        for insight in sample_insights:
            st.markdown(f"- {insight}")
    aba_idx += 1
    if map_insights:
        with resumo_tabs[aba_idx]:
            for insight in map_insights:
                st.markdown(f"- {insight}")
        aba_idx += 1
    if tema_insights:
        with resumo_tabs[aba_idx]:
            if tema_escolhido:
                st.caption(f"Leitura guiada do tema: {tema_escolhido}")
            for insight in tema_insights:
                st.markdown(f"- {insight}")

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
