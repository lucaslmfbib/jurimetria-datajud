from datetime import date, datetime, time as dt_time
from functools import lru_cache
import html
import io
import importlib
import os
import re
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
FAST_COMPLEMENTARY_SKIP_THRESHOLD = 300
FAST_DECISION_SAMPLE_LIMIT = 250
FAST_MAP_SAMPLE_LIMIT = 800
THEME_DIRECT_FAST_DECISION_LIMIT = 180
THEME_DIRECT_TIMEOUT_SECONDS = 30
STRATEGY_RELOAD_MIN_SIZE = 1200
STRATEGY_RELOAD_MAX_SIZE = 3000
THEME_SUGGESTION_SAMPLE_SIZE = 800
THEME_SUGGESTION_MAX_ITEMS = 500
THEME_SUGGESTION_TIMEOUT_SECONDS = 18
APP_VERSION_LABEL = "Versao esperada: 17/04/2026 | Ajuste de Classes com mais processos"
DECISION_SOURCE_FIELDS = [
    "numeroProcesso",
    "classe.codigo",
    "classe.nome",
    "dataAjuizamento",
    "orgaoJulgador.nome",
    "orgaoJulgador.codigoMunicipioIBGE",
    "grau",
    "assuntos",
    "movimentos",
]

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

COMPARISON_DIMENSIONS = {
    "orgao": {
        "label": "Orgao completo",
        "column": "comparativo_orgao",
        "axis_label": "Orgao julgador",
        "table_label": "Orgao completo",
        "plural_label": "orgaos completos",
    },
    "vara": {
        "label": "Vara/juizo normalizado",
        "column": "comparativo_vara",
        "axis_label": "Vara/juizo ou unidade equivalente",
        "table_label": "Vara/juizo normalizado",
        "plural_label": "varas/juizos normalizados",
    },
    "comarca": {
        "label": "Municipio/comarca",
        "column": "comparativo_comarca",
        "axis_label": "Municipio/comarca",
        "table_label": "Municipio/comarca",
        "plural_label": "municipios/comarcas",
    },
}

UNIDADE_NORMALIZADA_PATTERNS = (
    r"(\d+\s*(?:a|o)?\s*zona eleitoral\b[^,;/()]*)",
    r"(\d+\s*(?:a|o)?\s*vara\b[^,;/()]*)",
    r"(vara unica\b[^,;/()]*)",
    r"(\d+\s*(?:a|o)?\s*juizado\b[^,;/()]*)",
    r"(juizado especial\b[^,;/()]*)",
    r"(juizado da fazenda publica\b[^,;/()]*)",
    r"(turma recursal\b[^,;/()]*)",
    r"(colegio recursal\b[^,;/()]*)",
    r"(auditoria\b[^,;/()]*)",
    r"(juizo\b[^,;/()]*)",
    r"(camara\b[^,;/()]*)",
    r"(turma\b[^,;/()]*)",
    r"(secao\b[^,;/()]*)",
    r"(gabinete\b[^,;/()]*)",
    r"(vara\b[^,;/()]*)",
)

COMARCA_PATTERNS = (
    r"\bcomarca de ([a-z0-9 ]+)",
    r"\bforo de ([a-z0-9 ]+)",
    r"\bsubsecao judiciaria de ([a-z0-9 ]+)",
    r"\bmunicipio de ([a-z0-9 ]+)",
)

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


def normalize_assunto_filtro(raw_assunto: Any) -> str:
    return str(raw_assunto or "").strip()


def build_theme_suggestion_cache_key(
    tribunal_sigla: str,
    classe_codigo: Any,
    estrutura_filtro: str,
    data_inicio: Any = None,
    data_fim: Any = None,
) -> tuple[str, int, str, str, str]:
    inicio = format_periodo_aplicado(data_inicio, None)
    fim = format_periodo_aplicado(None, data_fim)
    return (
        normalize_tribunal_sigla(tribunal_sigla),
        int(classe_codigo or 0),
        str(estrutura_filtro or "Todos"),
        str(inicio or ""),
        str(fim or ""),
    )


def sync_tema_text_from_select() -> None:
    tema_escolhido = normalize_assunto_filtro(st.session_state.get("tema_consulta_select", ""))
    st.session_state["tema_consulta_text_fallback"] = tema_escolhido


def current_query_can_seed_theme_suggestions(
    classe_codigo: Any,
    tribunal_sigla: str,
    estrutura_filtro: str,
    data_inicio: Any = None,
    data_fim: Any = None,
) -> bool:
    query_context = dict(st.session_state.get("last_query_context", {}))
    df_anpp = st.session_state.get("df_anpp", pd.DataFrame())
    if not isinstance(df_anpp, pd.DataFrame) or df_anpp.empty:
        return False
    if not isinstance(query_context, dict):
        return False
    if bool(query_context.get("usar_numero_processo", False)):
        return False
    if bool(query_context.get("busca_tema_direto", False)):
        return False
    if normalize_assunto_filtro(query_context.get("tema_consulta", "")):
        return False
    if int(query_context.get("classe_codigo", 0) or 0) != int(classe_codigo or 0):
        return False
    if normalize_tribunal_sigla(query_context.get("tribunal_sigla", "")) != normalize_tribunal_sigla(tribunal_sigla):
        return False
    if str(query_context.get("estrutura_filtro", "Todos")) != str(estrutura_filtro or "Todos"):
        return False
    periodo_contexto = format_periodo_aplicado(
        query_context.get("data_inicio_consulta"),
        query_context.get("data_fim_consulta"),
    )
    periodo_atual = format_periodo_aplicado(data_inicio, data_fim)
    return periodo_contexto == periodo_atual


def build_theme_suggestions_from_current_query(max_items: int = THEME_SUGGESTION_MAX_ITEMS) -> pd.DataFrame:
    df_anpp = st.session_state.get("df_anpp", pd.DataFrame())
    if not isinstance(df_anpp, pd.DataFrame) or df_anpp.empty:
        return pd.DataFrame(columns=["assunto", "quantidade"])
    return assuntos_distintos_dataframe(df_anpp).head(max_items).reset_index(drop=True)


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


@lru_cache(maxsize=16384)
def _normalize_search_text_cached(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")


def normalize_search_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return _normalize_search_text_cached(text)


def unique_assuntos_list(assuntos: Any) -> list[str]:
    if not isinstance(assuntos, list):
        return []

    temas: list[str] = []
    vistos: set[str] = set()
    for assunto in assuntos:
        tema = str(assunto or "").strip()
        if not tema or tema in vistos:
            continue
        vistos.add(tema)
        temas.append(tema)
    return temas


def clean_normalized_text(text: Any) -> str:
    normalized = normalize_search_text(text)
    normalized = re.sub(r"[\(\)\[\];]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip(" -/,")
    return normalized


def humanize_comparison_label(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip(" -/,")
    if not cleaned:
        return ""
    preposicoes = {"da", "de", "do", "das", "dos", "e"}
    partes = []
    for palavra in cleaned.split():
        if palavra in preposicoes:
            partes.append(palavra)
        elif palavra and palavra[0].isdigit():
            partes.append(palavra)
        else:
            partes.append(palavra.capitalize())
    return " ".join(partes)


@lru_cache(maxsize=8192)
def normalized_unit_label(orgao_julgador: str) -> str:
    text = clean_normalized_text(orgao_julgador)
    if not text:
        return ""

    for pattern in UNIDADE_NORMALIZADA_PATTERNS:
        match = re.search(pattern, text)
        if not match:
            continue
        candidate = match.group(1).strip()
        candidate = re.sub(r"\b(?:da|do) comarca de .*$", "", candidate).strip()
        candidate = re.sub(r"\b(?:da|do) foro de .*$", "", candidate).strip()
        candidate = re.sub(r"\bda subsecao judiciaria de .*$", "", candidate).strip()
        return humanize_comparison_label(candidate)

    candidate = re.split(r"[,/\-]", text, maxsplit=1)[0].strip()
    return humanize_comparison_label(candidate or text)


@lru_cache(maxsize=8192)
def comarca_label_from_orgao(orgao_julgador: str) -> str:
    text = clean_normalized_text(orgao_julgador)
    if not text:
        return ""

    for pattern in COMARCA_PATTERNS:
        match = re.search(pattern, text)
        if not match:
            continue
        nome = humanize_comparison_label(match.group(1))
        if nome:
            return f"Comarca de {nome}"
    return ""


def municipio_comarca_label(municipio: Any, orgao_julgador: Any) -> str:
    comarca = comarca_label_from_orgao(str(orgao_julgador or ""))
    if comarca:
        return comarca

    codigo = str(municipio or "").strip()
    if codigo:
        return f"Municipio {codigo}"

    return normalized_unit_label(str(orgao_julgador or ""))


def add_comparison_columns(df_anpp: pd.DataFrame) -> pd.DataFrame:
    if df_anpp.empty:
        return df_anpp.copy()

    df_out = df_anpp.copy()
    orgaos = (
        df_out["orgao_julgador"].fillna("").astype(str).str.strip()
        if "orgao_julgador" in df_out.columns
        else pd.Series([""] * len(df_out), index=df_out.index)
    )
    municipios = (
        df_out["municipio"].tolist()
        if "municipio" in df_out.columns
        else [None] * len(df_out)
    )

    df_out["comparativo_orgao"] = orgaos
    df_out["comparativo_vara"] = [normalized_unit_label(orgao) for orgao in orgaos.tolist()]
    df_out["comparativo_comarca"] = [
        municipio_comarca_label(municipio, orgao)
        for municipio, orgao in zip(municipios, orgaos.tolist())
    ]
    df_out["comparativo_vara"] = df_out["comparativo_vara"].where(
        df_out["comparativo_vara"].astype(str).str.strip().ne(""),
        df_out["comparativo_orgao"],
    )
    df_out["comparativo_comarca"] = df_out["comparativo_comarca"].where(
        df_out["comparativo_comarca"].astype(str).str.strip().ne(""),
        df_out["comparativo_orgao"],
    )
    return df_out


@lru_cache(maxsize=16384)
def is_decisive_movement_name(nome: str) -> bool:
    text = normalize_search_text(nome)
    if not text:
        return False
    if any(hint in text for hint in MOVIMENTO_NAO_DECISORIO_HINTS):
        return False
    return any(hint in text for hint in DECISAO_MOVIMENTO_HINTS)


@lru_cache(maxsize=16384)
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


def decision_by_orgao_dataframe(
    df_anpp: pd.DataFrame,
    max_orgaos: int = 10,
    group_column: str = "orgao_julgador",
) -> pd.DataFrame:
    columns = [
        "orgao_julgador",
        "processos_tema",
        "com_desfecho",
        "cobertura",
        "desfecho_predominante",
        "forca_predominante",
        "mediana_dias",
    ]
    if df_anpp.empty or group_column not in df_anpp.columns:
        return pd.DataFrame(columns=columns)

    base = df_anpp[[group_column, "decisao_categoria", "dias_ate_decisao_proxy"]].copy()
    base[group_column] = base[group_column].fillna("").astype(str).str.strip()
    base = base[base[group_column] != ""]
    if base.empty:
        return pd.DataFrame(columns=columns)

    top_orgaos = base[group_column].value_counts().head(max_orgaos)
    ordem_orgaos = {orgao: idx for idx, orgao in enumerate(top_orgaos.index)}
    base = base[base[group_column].isin(top_orgaos.index)].copy()
    base["decisao_categoria"] = base["decisao_categoria"].fillna("").astype(str).str.strip()
    base["dias_ate_decisao_proxy"] = pd.to_numeric(base["dias_ate_decisao_proxy"], errors="coerce")

    resultado = top_orgaos.rename_axis("orgao_julgador").reset_index(name="processos_tema")

    com_desfecho = base.loc[base["decisao_categoria"] != ""].groupby(group_column).size()
    resultado["com_desfecho"] = (
        pd.to_numeric(resultado["orgao_julgador"].map(com_desfecho), errors="coerce")
        .fillna(0)
        .astype(int)
    )
    resultado["cobertura"] = (
        (resultado["com_desfecho"] / resultado["processos_tema"] * 100)
        .round(1)
        .map(lambda valor: f"{valor:.1f}%")
    )

    desfechos_validos = base.loc[base["decisao_categoria"] != "", [group_column, "decisao_categoria"]]
    if desfechos_validos.empty:
        resultado["desfecho_predominante"] = "Sem leitura"
        resultado["forca_predominante"] = "-"
    else:
        predominantes = (
            desfechos_validos.groupby([group_column, "decisao_categoria"], as_index=False)
            .size()
            .sort_values(
                [group_column, "size", "decisao_categoria"],
                ascending=[True, False, True],
            )
            .drop_duplicates(subset=[group_column])
            .rename(
                columns={
                    group_column: "orgao_julgador",
                    "decisao_categoria": "desfecho_predominante",
                    "size": "quantidade_predominante",
                }
            )
        )
        resultado = resultado.merge(
            predominantes[["orgao_julgador", "desfecho_predominante", "quantidade_predominante"]],
            on="orgao_julgador",
            how="left",
        )
        resultado["desfecho_predominante"] = resultado["desfecho_predominante"].fillna("Sem leitura")
        resultado["forca_predominante"] = "-"
        mask_forca = resultado["quantidade_predominante"].notna() & resultado["com_desfecho"].gt(0)
        resultado.loc[mask_forca, "forca_predominante"] = (
            (
                resultado.loc[mask_forca, "quantidade_predominante"]
                / resultado.loc[mask_forca, "com_desfecho"]
                * 100
            )
            .round(1)
            .map(lambda valor: f"{valor:.1f}%")
        )
        resultado = resultado.drop(columns=["quantidade_predominante"], errors="ignore")

    medianas = base.groupby(group_column)["dias_ate_decisao_proxy"].median()
    resultado["mediana_dias"] = pd.to_numeric(
        resultado["orgao_julgador"].map(medianas),
        errors="coerce",
    ).round(4)
    resultado["mediana_dias"] = resultado["mediana_dias"].where(
        resultado["mediana_dias"].notna(),
        pd.NA,
    )
    resultado["ordem"] = resultado["orgao_julgador"].map(ordem_orgaos)
    return (
        resultado.sort_values("ordem")
        .drop(columns=["ordem"], errors="ignore")
        .reset_index(drop=True)[columns]
    )


def decision_signal_base_dataframe(
    df_anpp: pd.DataFrame,
    group_column: str = "orgao_julgador",
) -> pd.DataFrame:
    if df_anpp.empty or group_column not in df_anpp.columns or "decisao_categoria" not in df_anpp.columns:
        return pd.DataFrame(columns=["orgao_julgador", "decisao_categoria"])

    base = df_anpp[[group_column, "decisao_categoria"]].copy()
    base[group_column] = base[group_column].fillna("").astype(str).str.strip()
    base["decisao_categoria"] = base["decisao_categoria"].fillna("").astype(str).str.strip()
    base = base[(base[group_column] != "") & (base["decisao_categoria"] != "")]
    if base.empty:
        return pd.DataFrame(columns=["orgao_julgador", "decisao_categoria"])

    base = base.rename(columns={group_column: "orgao_julgador"})
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


def decision_polarity_base_dataframe(
    df_anpp: pd.DataFrame,
    group_column: str = "orgao_julgador",
) -> pd.DataFrame:
    base = decision_signal_base_dataframe(df_anpp, group_column=group_column)
    if base.empty:
        return pd.DataFrame(columns=["orgao_julgador", "decisao_categoria", "polaridade"])
    base = base.copy()
    base["polaridade"] = base["decisao_categoria"].map(outcome_polarity_label)
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

    polaridades = categorias.map(outcome_polarity_label).value_counts()
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
    group_column: str = "orgao_julgador",
) -> pd.DataFrame:
    columns = [
        "orgao_julgador",
        "decisoes_classificadas",
        "decisoes_uteis",
        "favoravel_pct",
        "desfavoravel_pct",
        "misto_pct",
        "indice_favorabilidade",
        "leitura_favorabilidade",
    ]
    base = decision_polarity_base_dataframe(df_anpp, group_column=group_column)
    if base.empty:
        return pd.DataFrame(columns=columns)

    contagem_polaridades = pd.crosstab(base["orgao_julgador"], base["polaridade"]).reindex(
        columns=[POLARIDADE_FAVORAVEL, POLARIDADE_DESFAVORAVEL, POLARIDADE_MISTA],
        fill_value=0,
    )
    resultado = contagem_polaridades.reset_index().rename(
        columns={
            POLARIDADE_FAVORAVEL: "favoravel_qtd",
            POLARIDADE_DESFAVORAVEL: "desfavoravel_qtd",
            POLARIDADE_MISTA: "misto_qtd",
        }
    )
    resultado["decisoes_classificadas"] = (
        base.groupby("orgao_julgador").size().reindex(resultado["orgao_julgador"]).to_numpy()
    )
    resultado["decisoes_uteis"] = (
        resultado["favoravel_qtd"] + resultado["desfavoravel_qtd"] + resultado["misto_qtd"]
    )
    resultado = resultado[resultado["decisoes_uteis"] >= int(min_decisoes_uteis)].copy()
    if resultado.empty:
        return pd.DataFrame(columns=columns)

    indice = (
        (resultado["favoravel_qtd"] + (0.5 * resultado["misto_qtd"]) - resultado["desfavoravel_qtd"])
        / resultado["decisoes_uteis"]
        * 100
    )
    resultado["favoravel_pct"] = (resultado["favoravel_qtd"] / resultado["decisoes_uteis"] * 100).round(1)
    resultado["desfavoravel_pct"] = (resultado["desfavoravel_qtd"] / resultado["decisoes_uteis"] * 100).round(1)
    resultado["misto_pct"] = (resultado["misto_qtd"] / resultado["decisoes_uteis"] * 100).round(1)
    resultado["indice_favorabilidade"] = indice.round(1)
    resultado["leitura_favorabilidade"] = resultado["indice_favorabilidade"].map(
        lambda valor: favorability_label_from_index(float(valor)) if pd.notna(valor) else "Sem base"
    )
    resultado = resultado.sort_values(
        ["indice_favorabilidade", "decisoes_uteis"],
        ascending=[False, False],
    )
    if max_items is not None:
        resultado = resultado.head(max_items)
    return resultado.reset_index(drop=True)[columns]


def decision_favorability_by_orgao_with_fallback(
    df_anpp: pd.DataFrame,
    preferred_min_decisoes_uteis: int = 5,
    max_items: int | None = 12,
    group_column: str = "orgao_julgador",
) -> tuple[pd.DataFrame, int]:
    thresholds = [preferred_min_decisoes_uteis, 3, 2, 1]
    thresholds = list(dict.fromkeys(int(valor) for valor in thresholds if int(valor) > 0))

    for min_decisoes in thresholds:
        resultado = decision_favorability_by_orgao_dataframe(
            df_anpp,
            min_decisoes_uteis=min_decisoes,
            max_items=max_items,
            group_column=group_column,
        )
        if not resultado.empty:
            return resultado, min_decisoes

    return decision_favorability_by_orgao_dataframe(
        df_anpp,
        min_decisoes_uteis=preferred_min_decisoes_uteis,
        max_items=max_items,
        group_column=group_column,
    ), preferred_min_decisoes_uteis


def decision_time_by_orgao_dataframe(
    df_anpp: pd.DataFrame,
    min_processos: int = 3,
    max_items: int | None = 12,
    group_column: str = "orgao_julgador",
) -> pd.DataFrame:
    columns = ["orgao_julgador", "processos_com_tempo", "mediana_dias", "p75_dias"]
    if df_anpp.empty or group_column not in df_anpp.columns or "dias_ate_decisao_proxy" not in df_anpp.columns:
        return pd.DataFrame(columns=columns)

    base = df_anpp[[group_column, "dias_ate_decisao_proxy"]].copy()
    base[group_column] = base[group_column].fillna("").astype(str).str.strip()
    base["dias_ate_decisao_proxy"] = pd.to_numeric(base["dias_ate_decisao_proxy"], errors="coerce")
    base = base[(base[group_column] != "") & (base["dias_ate_decisao_proxy"].notna())]
    if base.empty:
        return pd.DataFrame(columns=columns)

    resultado = (
        base.groupby(group_column, as_index=False)
        .agg(
            processos_com_tempo=("dias_ate_decisao_proxy", "size"),
            mediana_dias=("dias_ate_decisao_proxy", "median"),
            p75_dias=("dias_ate_decisao_proxy", lambda serie: serie.quantile(0.75)),
        )
    )
    resultado = resultado.rename(columns={group_column: "orgao_julgador"})
    resultado = resultado[resultado["processos_com_tempo"] >= int(min_processos)].copy()
    if resultado.empty:
        return pd.DataFrame(columns=columns)

    resultado["mediana_dias"] = pd.to_numeric(resultado["mediana_dias"], errors="coerce").round(4)
    resultado["p75_dias"] = pd.to_numeric(resultado["p75_dias"], errors="coerce").round(4)
    resultado = resultado.sort_values(
        ["mediana_dias", "processos_com_tempo"],
        ascending=[True, False],
    )
    if max_items is not None:
        resultado = resultado.head(max_items)
    return resultado.reset_index(drop=True)[columns]


def decision_time_by_orgao_with_fallback(
    df_anpp: pd.DataFrame,
    preferred_min_processos: int = 3,
    max_items: int | None = 12,
    group_column: str = "orgao_julgador",
) -> tuple[pd.DataFrame, int]:
    thresholds = [preferred_min_processos, 2, 1]
    thresholds = list(dict.fromkeys(int(valor) for valor in thresholds if int(valor) > 0))

    for min_processos in thresholds:
        resultado = decision_time_by_orgao_dataframe(
            df_anpp,
            min_processos=min_processos,
            max_items=max_items,
            group_column=group_column,
        )
        if not resultado.empty:
            return resultado, min_processos

    return decision_time_by_orgao_dataframe(
        df_anpp,
        min_processos=preferred_min_processos,
        max_items=max_items,
        group_column=group_column,
    ), preferred_min_processos


def decision_stability_by_orgao_dataframe(
    df_anpp: pd.DataFrame,
    min_classificados: int = 5,
    max_items: int | None = 12,
    group_column: str = "orgao_julgador",
) -> pd.DataFrame:
    columns = [
        "orgao_julgador",
        "decisoes_classificadas",
        "desfecho_lider",
        "forca_lider",
        "indice_estabilidade",
        "perfil_estabilidade",
    ]
    base = decision_signal_base_dataframe(df_anpp, group_column=group_column)
    if base.empty:
        return pd.DataFrame(columns=columns)

    contagem = (
        base.groupby(["orgao_julgador", "decisao_categoria"], as_index=False)
        .size()
        .rename(columns={"size": "quantidade"})
    )
    totais = contagem.groupby("orgao_julgador")["quantidade"].sum().rename("decisoes_classificadas")
    contagem = contagem.merge(totais, on="orgao_julgador", how="left")
    contagem = contagem[contagem["decisoes_classificadas"] >= int(min_classificados)].copy()
    if contagem.empty:
        return pd.DataFrame(columns=columns)

    contagem["share_quadrado"] = (
        contagem["quantidade"] / contagem["decisoes_classificadas"]
    ).pow(2)
    indices = (
        contagem.groupby("orgao_julgador", as_index=False)["share_quadrado"]
        .sum()
        .rename(columns={"share_quadrado": "indice_estabilidade"})
    )
    indices["indice_estabilidade"] = (indices["indice_estabilidade"] * 100).round(1)

    lideres = (
        contagem.sort_values(
            ["orgao_julgador", "quantidade", "decisao_categoria"],
            ascending=[True, False, True],
        )
        .drop_duplicates(subset=["orgao_julgador"])
        .rename(columns={"decisao_categoria": "desfecho_lider", "quantidade": "quantidade_lider"})
    )

    resultado = (
        contagem[["orgao_julgador", "decisoes_classificadas"]]
        .drop_duplicates(subset=["orgao_julgador"])
        .merge(lideres[["orgao_julgador", "desfecho_lider", "quantidade_lider"]], on="orgao_julgador", how="left")
        .merge(indices, on="orgao_julgador", how="left")
    )
    resultado["forca_lider"] = (
        resultado["quantidade_lider"] / resultado["decisoes_classificadas"] * 100
    ).round(1)
    resultado["perfil_estabilidade"] = resultado["indice_estabilidade"].map(
        lambda valor: stability_label_from_index(float(valor)) if pd.notna(valor) else "Sem base"
    )
    resultado = resultado.sort_values(
        ["indice_estabilidade", "decisoes_classificadas"],
        ascending=[False, False],
    )
    if max_items is not None:
        resultado = resultado.head(max_items)
    return resultado.reset_index(drop=True).drop(columns=["quantidade_lider"], errors="ignore")[columns]


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
    recorte_label_plural: str = "orgaos julgadores",
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
            f"Ainda nao ha {recorte_label_plural} suficientes com base util para um ranking robusto de favorabilidade."
        )
    if not isinstance(tempo_orgaos, pd.DataFrame) or tempo_orgaos.empty:
        alertas.append(
            f"Ainda nao ha massa critica para comparar tempo mediano de decisao por {recorte_label_plural} neste tema."
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
    group_column: str = "orgao_julgador",
) -> pd.DataFrame:
    base = decision_signal_base_dataframe(df_anpp, group_column=group_column)
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


def outcome_mix_profile_summary(df_mix: pd.DataFrame) -> dict[str, Any]:
    if df_mix.empty or "orgao_julgador" not in df_mix.columns:
        return {
            "uniforme": False,
            "desfecho_dominante": "",
            "desfechos_ativos": 0,
            "perfis_unicos": 0,
        }

    base = df_mix.copy()
    colunas_desfecho = [
        coluna for coluna in base.columns if coluna not in {"orgao_julgador", "total_classificados"}
    ]
    if not colunas_desfecho:
        return {
            "uniforme": False,
            "desfecho_dominante": "",
            "desfechos_ativos": 0,
            "perfis_unicos": 0,
        }

    base = base[base["total_classificados"] > 0].copy()
    if base.empty:
        return {
            "uniforme": False,
            "desfecho_dominante": "",
            "desfechos_ativos": 0,
            "perfis_unicos": 0,
        }

    percentuais = base[colunas_desfecho].div(base["total_classificados"], axis=0).fillna(0.0)
    desfechos_ativos = [coluna for coluna in colunas_desfecho if float(base[coluna].sum()) > 0]
    perfis_unicos = int(percentuais.round(3).drop_duplicates().shape[0])
    desfecho_dominante = ""
    if desfechos_ativos:
        desfecho_dominante = max(desfechos_ativos, key=lambda coluna: float(base[coluna].sum()))

    return {
        "uniforme": bool(len(desfechos_ativos) <= 1 or perfis_unicos <= 1),
        "desfecho_dominante": str(desfecho_dominante),
        "desfechos_ativos": int(len(desfechos_ativos)),
        "perfis_unicos": perfis_unicos,
    }


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
    columns = [
        "tema",
        "processos",
        "com_desfecho",
        "cobertura_desfecho",
        "com_movimento_final",
        "cobertura_movimento",
    ]
    if df_anpp.empty or "assuntos" not in df_anpp.columns:
        return pd.DataFrame(columns=columns)

    base = df_anpp[["assuntos", "decisao_categoria", "decisao_movimento"]].copy()
    base["tema"] = base["assuntos"].apply(unique_assuntos_list)
    base = base.explode("tema")
    base["tema"] = base["tema"].fillna("").astype(str).str.strip()
    base = base[base["tema"] != ""]
    if base.empty:
        return pd.DataFrame(columns=columns)

    base["processos"] = 1
    base["com_desfecho"] = (
        base["decisao_categoria"].fillna("").astype(str).str.strip().ne("").astype(int)
    )
    base["com_movimento_final"] = (
        base["decisao_movimento"].fillna("").astype(str).str.strip().ne("").astype(int)
    )
    overview = (
        base.groupby("tema", as_index=False)[["processos", "com_desfecho", "com_movimento_final"]]
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
    return overview[columns]


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
    timeout_seconds: int = DATAJUD_TIMEOUT_SECONDS,
) -> requests.Response:
    last_error: Exception | None = None
    for tentativa in range(DATAJUD_MAX_RETRIES + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout_seconds,
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
    assunto_nome: str = "",
    data_inicio: Any = None,
    data_fim: Any = None,
    incluir_movimentos: bool = False,
    modo_consulta: str = "classe_ou_processo",
    source_fields: list[str] | None = None,
    timeout_seconds: int = DATAJUD_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    numero_limpo = normalize_numero_processo(numero_processo)
    assunto_limpo = normalize_assunto_filtro(assunto_nome)
    filtros: list[dict[str, Any]] = []
    if numero_limpo:
        filtros.append({"match": {"numeroProcesso": numero_limpo}})
    elif modo_consulta == "mapa_tribunal":
        if assunto_limpo:
            filtros.append({"match_phrase": {"assuntos.nome": assunto_limpo}})
    elif modo_consulta == "tema_direto":
        if assunto_limpo:
            filtros.append({"match_phrase": {"assuntos.nome": assunto_limpo}})
    else:
        filtros.append({"match": {"classe.codigo": classe_codigo}})
        if assunto_limpo:
            filtros.append({"match_phrase": {"assuntos.nome": assunto_limpo}})

    filtro_data = build_data_ajuizamento_range(data_inicio=data_inicio, data_fim=data_fim)
    if filtro_data:
        filtros.append(filtro_data)

    if filtros:
        query: dict[str, Any] = {"bool": {"filter": filtros}}
    else:
        query = {"match_all": {}}

    campos_source = list(source_fields) if source_fields else [
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
    if incluir_movimentos and "movimentos" not in campos_source:
        campos_source.append("movimentos")

    standard_sort = [{"dataAjuizamento": {"order": "desc"}}]
    use_unsorted_theme_direct = (
        modo_consulta == "tema_direto"
        and not numero_limpo
        and size <= MAX_PAGE_SIZE
    )
    payload = {
        "size": size,
        "_source": campos_source,
        "query": query,
        "track_total_hits": False,
    }
    if not use_unsorted_theme_direct:
        payload["sort"] = standard_sort
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
            timeout_seconds=timeout_seconds,
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
            "track_total_hits": False,
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
            timeout_seconds=timeout_seconds,
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


def top_comarcas_dataframe(df_anpp: pd.DataFrame, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty:
        return pd.DataFrame(columns=["municipio_comarca", "quantidade", "participacao"])

    orgaos = (
        df_anpp["orgao_julgador"].fillna("").astype(str).str.strip()
        if "orgao_julgador" in df_anpp.columns
        else pd.Series([""] * len(df_anpp), index=df_anpp.index)
    )
    municipios = (
        df_anpp["municipio"].tolist()
        if "municipio" in df_anpp.columns
        else [None] * len(df_anpp)
    )
    labels = pd.Series(
        [municipio_comarca_label(municipio, orgao) for municipio, orgao in zip(municipios, orgaos.tolist())],
        index=df_anpp.index,
    ).fillna("").astype(str).str.strip()
    labels = labels[labels != ""]
    if labels.empty:
        return pd.DataFrame(columns=["municipio_comarca", "quantidade", "participacao"])

    total = int(len(labels))
    top = (
        labels.value_counts()
        .head(max_items)
        .rename_axis("municipio_comarca")
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


def shorten_display_label(value: Any, max_chars: int = 36) -> str:
    text = str(value or "").strip()
    if not text or len(text) <= max_chars:
        return text

    truncated = text[:max_chars].rsplit(" ", 1)[0].strip()
    if len(truncated) < max_chars // 2:
        truncated = text[:max_chars].strip()
    return f"{truncated}..."


def format_duration_label(value_in_days: Any) -> str:
    try:
        valor = float(value_in_days)
    except Exception:
        return "-"

    if pd.isna(valor):
        return "-"
    if valor < 0:
        return "-"
    if valor == 0:
        return "No mesmo dia"
    if valor < 1:
        return f"{valor * 24:.1f} h"
    if valor < 10:
        return f"{valor:.1f} dias"
    return f"{valor:.0f} dias"


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
    top_orgaos_sigla: pd.DataFrame,
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
    if isinstance(top_orgaos_sigla, pd.DataFrame) and not top_orgaos_sigla.empty:
        linha = top_orgaos_sigla.iloc[0]
        insights.append(
            f"O órgão julgador mais frequente no mapa da sigla é `{linha['orgao_julgador']}`, com {format_int_br(linha['quantidade'])} registros."
        )
    if isinstance(top_assuntos, pd.DataFrame) and not top_assuntos.empty:
        linha = top_assuntos.iloc[0]
        insights.append(
            f"O assunto mais recorrente no mapa da sigla é `{linha['assunto']}`, com {format_int_br(linha['quantidade'])} ocorrências."
        )
    return insights


def build_query_derived_state(
    df_anpp: pd.DataFrame,
    df_mensal: pd.DataFrame,
    top_100: pd.Series,
    top_codigos: pd.DataFrame,
    top_orgaos_sigla: pd.DataFrame,
    top_assuntos: pd.DataFrame,
    df_decisao: pd.DataFrame,
    qtd_mapa: int,
) -> dict[str, Any]:
    top_100_df = top_100_to_dataframe(top_100)
    top_orgaos_df = top_orgaos_julgadores_dataframe(df_anpp)
    top_comarcas_df = top_comarcas_dataframe(df_anpp)
    top_classes_df = top_classes_display_dataframe(df_anpp)
    assuntos_distintos = assuntos_distintos_dataframe(df_anpp)
    sample_insights = build_sample_insights(df_anpp, df_mensal, top_orgaos_df, top_100_df)
    map_insights = build_map_insights(top_codigos, top_orgaos_sigla, top_assuntos, qtd_mapa)

    temas_decisao = pd.DataFrame(columns=["assunto", "quantidade"])
    temas_overview = pd.DataFrame(
        columns=[
            "tema",
            "processos",
            "com_desfecho",
            "cobertura_desfecho",
            "com_movimento_final",
            "cobertura_movimento",
        ]
    )
    if isinstance(df_decisao, pd.DataFrame) and not df_decisao.empty:
        temas_decisao = assuntos_distintos_dataframe(df_decisao)
        temas_overview = theme_overview_dataframe(df_decisao)

    return {
        "df_view": dataframe_for_display(df_anpp, max_rows=400),
        "top_100_df": top_100_df,
        "top_orgaos_df": top_orgaos_df,
        "top_comarcas_df": top_comarcas_df,
        "top_classes_df": top_classes_df,
        "sample_insights": sample_insights,
        "map_insights": map_insights,
        "assuntos_distintos": assuntos_distintos,
        "total_assuntos": int(len(assuntos_distintos)),
        "temas_decisao": temas_decisao,
        "temas_overview": temas_overview,
    }


def strategy_reload_target_size(query_size: Any, qtd_decisao_atual: Any = 0) -> int:
    query_size_int = max(int(query_size or 0), 0)
    qtd_decisao_int = max(int(qtd_decisao_atual or 0), 0)
    target_size = max(
        query_size_int,
        qtd_decisao_int * 2,
        STRATEGY_RELOAD_MIN_SIZE,
    )
    return min(target_size, STRATEGY_RELOAD_MAX_SIZE, MAX_PAGE_SIZE)


def fetch_strategy_decision_dataframe(
    api_key: str,
    query_context: dict[str, Any],
    target_size: int | None = None,
) -> tuple[pd.DataFrame, int]:
    classe_codigo = int(query_context.get("classe_codigo", 0) or 0)
    busca_tema_direto = bool(query_context.get("busca_tema_direto", False))
    url = str(query_context.get("url", "")).strip()
    tribunal_sigla = str(query_context.get("tribunal_sigla", "")).strip()
    estrutura_filtro = str(query_context.get("estrutura_filtro", "Todos"))
    query_size = int(query_context.get("query_size", 0) or 0)
    qtd_decisao_atual = int(query_context.get("qtd_decisao", 0) or 0)
    decision_size = int(target_size or strategy_reload_target_size(query_size, qtd_decisao_atual))
    timeout_seconds = THEME_DIRECT_TIMEOUT_SECONDS if busca_tema_direto else DATAJUD_TIMEOUT_SECONDS

    if not api_key or not url or not tribunal_sigla or (not busca_tema_direto and not classe_codigo):
        raise ValueError("Nao encontrei os parametros da ultima consulta para ampliar a leitura estrategica.")

    hits_decisao = fetch_hits(
        api_key=api_key,
        classe_codigo=classe_codigo,
        size=decision_size,
        url=url,
        numero_processo="",
        assunto_nome=query_context.get("tema_consulta", ""),
        data_inicio=query_context.get("data_inicio_consulta"),
        data_fim=query_context.get("data_fim_consulta"),
        incluir_movimentos=True,
        modo_consulta="tema_direto" if busca_tema_direto else "classe_ou_processo",
        source_fields=DECISION_SOURCE_FIELDS,
        timeout_seconds=timeout_seconds,
    )
    df_decisao = hits_to_dataframe(hits_decisao, processar_movimentos=True)
    if df_decisao.empty:
        return df_decisao, decision_size

    df_decisao = enrich_decision_proxy_dataframe(df_decisao)
    df_decisao = filter_dataframe_by_estrutura(df_decisao, tribunal_sigla, estrutura_filtro)
    df_decisao = add_comparison_columns(df_decisao)
    return df_decisao, decision_size


def replace_decision_state_in_session(
    df_decisao: pd.DataFrame,
    target_size: int,
    aviso: str | None = None,
) -> None:
    qtd_decisao = int(len(df_decisao))
    st.session_state["df_decisao"] = df_decisao
    st.session_state["qtd_decisao"] = qtd_decisao

    query_context = dict(st.session_state.get("last_query_context", {}))
    query_context["qtd_decisao"] = qtd_decisao
    query_context["strategy_target_size"] = int(target_size)
    st.session_state["last_query_context"] = query_context

    if aviso:
        avisos_consulta = list(st.session_state.get("avisos_consulta", []))
        if aviso not in avisos_consulta:
            avisos_consulta.append(aviso)
        st.session_state["avisos_consulta"] = avisos_consulta[-6:]

    st.session_state["derived_state"] = build_query_derived_state(
        df_anpp=st.session_state.get("df_anpp", pd.DataFrame()),
        df_mensal=st.session_state.get("df_mensal", pd.DataFrame()),
        top_100=st.session_state.get("top_100", pd.Series(dtype="int64")),
        top_codigos=st.session_state.get("top_codigos", pd.DataFrame()),
        top_orgaos_sigla=st.session_state.get("top_orgaos_sigla", pd.DataFrame()),
        top_assuntos=st.session_state.get("top_assuntos", pd.DataFrame()),
        df_decisao=df_decisao,
        qtd_mapa=int(st.session_state.get("qtd_mapa", 0) or 0),
    )


def build_comparison_dimension_state(df_tema_decisao: pd.DataFrame) -> dict[str, dict[str, Any]]:
    comparison_state: dict[str, dict[str, Any]] = {}
    for key, config in COMPARISON_DIMENSIONS.items():
        group_column = str(config["column"])
        orgaos_tema = decision_by_orgao_dataframe(
            df_tema_decisao,
            group_column=group_column,
        )
        mix_orgaos_tema = decision_outcome_mix_by_orgao_dataframe(
            df_tema_decisao,
            group_column=group_column,
        )
        favorabilidade_orgaos, favorabilidade_minima = decision_favorability_by_orgao_with_fallback(
            df_tema_decisao,
            preferred_min_decisoes_uteis=5,
            max_items=None,
            group_column=group_column,
        )
        tempo_orgaos, tempo_minimo = decision_time_by_orgao_with_fallback(
            df_tema_decisao,
            preferred_min_processos=3,
            max_items=None,
            group_column=group_column,
        )
        estabilidade_orgaos = decision_stability_by_orgao_dataframe(
            df_tema_decisao,
            min_classificados=5,
            max_items=None,
            group_column=group_column,
        )
        score = (
            len(favorabilidade_orgaos) * 1000
            + int(favorabilidade_orgaos["decisoes_uteis"].sum()) * 10
            + len(tempo_orgaos) * 250
            + len(estabilidade_orgaos) * 100
            + len(orgaos_tema) * 10
        )
        comparison_state[key] = {
            "orgaos_tema": orgaos_tema,
            "mix_orgaos_tema": mix_orgaos_tema,
            "favorabilidade_orgaos": favorabilidade_orgaos,
            "favorabilidade_minima": favorabilidade_minima,
            "tempo_orgaos": tempo_orgaos,
            "tempo_minimo": tempo_minimo,
            "estabilidade_orgaos": estabilidade_orgaos,
            "grupos_favorabilidade": int(len(favorabilidade_orgaos)),
            "grupos_tempo": int(len(tempo_orgaos)),
            "decisoes_uteis": int(favorabilidade_orgaos["decisoes_uteis"].sum())
            if not favorabilidade_orgaos.empty
            else 0,
            "score": int(score),
        }
    return comparison_state


def recommended_comparison_dimension(
    comparison_state: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    default_key = "orgao"
    if not comparison_state:
        return default_key, {"score": 0, "grupos_favorabilidade": 0, "grupos_tempo": 0, "decisoes_uteis": 0}

    order = list(COMPARISON_DIMENSIONS.keys())
    best_key = default_key
    best_rank = (-1, -1, -1, -1)
    for key in order:
        state = comparison_state.get(key, {})
        rank = (
            int(state.get("score", 0) or 0),
            int(state.get("grupos_favorabilidade", 0) or 0),
            int(state.get("grupos_tempo", 0) or 0),
            int(state.get("decisoes_uteis", 0) or 0),
        )
        if rank > best_rank:
            best_key = key
            best_rank = rank

    return best_key, comparison_state.get(best_key, {})


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


def top_classes_display_dataframe(
    df_anpp: pd.DataFrame,
    max_items: int = 10,
    max_chars: int = 38,
) -> pd.DataFrame:
    top_classes = top_classes_dataframe(df_anpp, max_items=max_items).copy()
    if top_classes.empty:
        return pd.DataFrame(columns=["classe", "quantidade"])

    top_classes["classe"] = top_classes["classe"].map(
        lambda classe: shorten_display_label(classe, max_chars=max_chars)
    )
    return top_classes


def top_assuntos_dataframe(df_anpp: pd.DataFrame, max_items: int = 10) -> pd.DataFrame:
    if df_anpp.empty or "assuntos" not in df_anpp.columns:
        return pd.DataFrame(columns=["assunto", "quantidade"])

    assuntos = df_anpp["assuntos"].explode().dropna().astype(str).str.strip()
    assuntos = assuntos[assuntos != ""]
    if assuntos.empty:
        return pd.DataFrame(columns=["assunto", "quantidade"])

    return assuntos.value_counts().head(max_items).rename_axis("assunto").reset_index(name="quantidade")


@st.cache_data(show_spinner=False, ttl=1200)
def fetch_theme_suggestions_dataframe(
    api_key: str,
    classe_codigo: int,
    url: str,
    tribunal_sigla: str,
    estrutura_filtro: str = "Todos",
    data_inicio: Any = None,
    data_fim: Any = None,
    sample_size: int = THEME_SUGGESTION_SAMPLE_SIZE,
    max_items: int = THEME_SUGGESTION_MAX_ITEMS,
    timeout_seconds: int = THEME_SUGGESTION_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    hits = fetch_hits(
        api_key=api_key,
        classe_codigo=int(classe_codigo),
        size=int(sample_size),
        url=url,
        numero_processo="",
        assunto_nome="",
        data_inicio=data_inicio,
        data_fim=data_fim,
        incluir_movimentos=False,
        modo_consulta="classe_ou_processo",
        timeout_seconds=timeout_seconds,
        source_fields=[
            "orgaoJulgador.nome",
            "grau",
            "assuntos",
        ],
    )
    df_sugestoes = hits_to_dataframe(hits, processar_movimentos=False)
    df_sugestoes = filter_dataframe_by_estrutura(df_sugestoes, tribunal_sigla, estrutura_filtro)
    return top_assuntos_dataframe(df_sugestoes, max_items=max_items)


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


def fig_desfechos_por_orgao(
    df_mix: pd.DataFrame,
    titulo: str = "Desfecho por orgao julgador",
    eixo_label: str = "Orgao julgador",
) -> Any:
    plt = get_plt()
    if df_mix.empty or "orgao_julgador" not in df_mix.columns:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
        ax.text(0.5, 0.5, "Sem dados suficientes para cruzar orgao e desfecho.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = df_mix.copy()
    colunas_desfecho = [
        coluna for coluna in base.columns if coluna not in {"orgao_julgador", "total_classificados"}
    ]
    if not colunas_desfecho:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
        ax.text(0.5, 0.5, "Sem desfechos classificados para montar o comparativo.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = base[base["total_classificados"] > 0].copy()
    if base.empty:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
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
    ax.set_ylabel(eixo_label)
    ax.set_title(titulo)
    ax.grid(axis="x", linestyle="--", alpha=0.25)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.35), ncol=2, frameon=False)
    fig.tight_layout()
    return fig


def fig_base_classificada_por_orgao(
    df_resumo: pd.DataFrame,
    titulo: str = "Base classificada por orgao julgador",
    eixo_label: str = "Orgao julgador",
) -> Any:
    plt = get_plt()
    if df_resumo.empty or "orgao_julgador" not in df_resumo.columns:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
        ax.text(0.5, 0.5, "Sem base suficiente para comparar cobertura por orgao.", ha="center", va="center")
        ax.axis("off")
        return fig

    colunas_necessarias = {"processos_tema", "com_desfecho"}
    if not colunas_necessarias.issubset(set(df_resumo.columns)):
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
        ax.text(0.5, 0.5, "Sem dados de volume e cobertura para este comparativo.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = df_resumo.copy()
    base["processos_tema"] = pd.to_numeric(base["processos_tema"], errors="coerce").fillna(0)
    base["com_desfecho"] = pd.to_numeric(base["com_desfecho"], errors="coerce").fillna(0)
    base = base[base["processos_tema"] > 0].copy()
    if base.empty:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
        ax.text(0.5, 0.5, "Sem processos suficientes para este comparativo.", ha="center", va="center")
        ax.axis("off")
        return fig

    base["cobertura_pct_num"] = (base["com_desfecho"] / base["processos_tema"] * 100).round(1)
    base = base.sort_values(
        ["com_desfecho", "cobertura_pct_num", "processos_tema"],
        ascending=[False, False, False],
    ).head(10)
    labels = [
        orgao if len(orgao) <= 30 else orgao[:30] + "..."
        for orgao in base["orgao_julgador"].astype(str)
    ]

    fig, ax = plt.subplots(figsize=(10.5, max(4.2, len(base) * 0.62 + 1.8)))
    ax.barh(labels, base["processos_tema"], color="#D7DCE5", alpha=0.95, label="Processos do tema")
    ax.barh(labels, base["com_desfecho"], color="#4E79A7", alpha=0.95, label="Com desfecho classificado")
    ax.invert_yaxis()
    ax.set_xlabel("Quantidade de processos")
    ax.set_ylabel(eixo_label)
    ax.set_title(titulo)
    ax.grid(axis="x", linestyle="--", alpha=0.25)

    max_valor = float(base["processos_tema"].max()) if not base.empty else 0.0
    deslocamento = max(max_valor * 0.015, 0.4)
    for i, (_, linha) in enumerate(base.iterrows()):
        ax.text(
            float(linha["processos_tema"]) + deslocamento,
            i,
            f"{int(linha['com_desfecho'])}/{int(linha['processos_tema'])} | {float(linha['cobertura_pct_num']):.1f}%",
            va="center",
            fontsize=9,
        )

    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.28), ncol=2, frameon=False)
    fig.tight_layout()
    return fig


def fig_favorabilidade_por_orgao(
    df_favorabilidade: pd.DataFrame,
    titulo: str = "Indice de favorabilidade por orgao",
    eixo_label: str = "Orgao julgador",
) -> Any:
    plt = get_plt()
    if df_favorabilidade.empty or "orgao_julgador" not in df_favorabilidade.columns:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
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
    ax.set_ylabel(eixo_label)
    ax.set_title(titulo)
    ax.grid(axis="x", linestyle="--", alpha=0.25)

    for i, valor in enumerate(valores):
        deslocamento = 1.2 if valor >= 0 else -1.2
        alinhamento = "left" if valor >= 0 else "right"
        ax.text(valor + deslocamento, i, f"{valor:.1f}", va="center", ha=alinhamento, fontsize=9)

    fig.tight_layout()
    return fig


def fig_tempo_por_orgao(
    df_tempo: pd.DataFrame,
    titulo: str = "Tempo mediano ate o primeiro desfecho identificado por orgao",
    eixo_label: str = "Orgao julgador",
) -> Any:
    plt = get_plt()
    if df_tempo.empty or "orgao_julgador" not in df_tempo.columns:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title(titulo)
        ax.text(0.5, 0.5, "Sem base suficiente para comparar tempo por orgao.", ha="center", va="center")
        ax.axis("off")
        return fig

    base = df_tempo.copy().sort_values("mediana_dias", ascending=True)
    labels = [
        orgao if len(orgao) <= 36 else orgao[:36] + "..."
        for orgao in base["orgao_julgador"].astype(str)
    ]
    valores = pd.to_numeric(base["mediana_dias"], errors="coerce").fillna(0.0)
    max_valor_dias = float(valores.max()) if not valores.empty else 0.0
    if max_valor_dias <= 0:
        fig, ax = plt.subplots(figsize=(10, 4.2))
        ax.set_title(titulo)
        ax.text(
            0.5,
            0.5,
            "Os tempos identificados ficaram no mesmo dia do ajuizamento nesta amostra.",
            ha="center",
            va="center",
        )
        ax.axis("off")
        return fig

    usar_horas = max_valor_dias < 1
    if usar_horas:
        valores_plot = valores * 24
        xlabel = "Mediana de horas ate o primeiro desfecho identificado"
        formatador = lambda valor: f"{valor:.1f} h"
    else:
        valores_plot = valores
        xlabel = "Mediana de dias ate o primeiro desfecho identificado"
        formatador = lambda valor: f"{valor:.1f}" if valor < 10 else f"{valor:.0f}"

    max_valor = float(valores_plot.max()) if not valores_plot.empty else 0.0
    margem_esquerda = min(0.42, 0.18 + (max((len(label) for label in labels), default=20) * 0.0055))

    fig, ax = plt.subplots(figsize=(12, max(4.8, len(base) * 0.72 + 1.8)))
    ax.barh(labels, valores_plot, color="#4E79A7", alpha=0.92)
    ax.invert_yaxis()
    ax.set_xlabel(xlabel)
    ax.set_ylabel(eixo_label)
    ax.set_title(titulo)
    ax.grid(axis="x", linestyle="--", alpha=0.25)
    ax.set_xlim(0, max(max_valor * 1.18, 1.0))
    ax.tick_params(axis="y", labelsize=9)
    ax.margins(y=0.03)

    for i, valor in enumerate(valores_plot):
        ax.text(
            valor + max(max_valor * 0.015, 0.8),
            i,
            formatador(float(valor)),
            va="center",
            fontsize=9,
        )

    fig.subplots_adjust(left=margem_esquerda, right=0.97, top=0.90, bottom=0.12)
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
    st.caption(APP_VERSION_LABEL)
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
        st.markdown("**1. Onde pesquisar**")
        tribunal_sigla = st.text_input(
            "Tribunal (sigla CNJ)",
            value="tjmg",
            help="Ex.: tjmg, tjmmg, trf1, trt3, stj, tst, tse, stm.",
        )
        st.markdown(f"[Consultar siglas de tribunais (CNJ)]({CNJ_SIGLAS_URL})")
        estrutura_info = get_estrutura_options(tribunal_sigla)
        estrutura_filtro = st.selectbox(
            "Recorte estrutural (opcional)",
            options=estrutura_info["opcoes"],
            index=0,
            format_func=format_estrutura_option,
            help="Use para separar a analise por grau, juizado, turma recursal ou estrutura equivalente.",
        )
        st.caption(describe_estrutura_option(estrutura_filtro))
        st.caption(str(estrutura_info["observacao"]))
        st.markdown("**2. Como pesquisar**")
        modo_busca_sidebar = st.radio(
            "Modo de busca",
            options=["classe", "tema", "processo"],
            format_func=lambda valor: {
                "classe": "Classe processual",
                "tema": "Tema no tribunal",
                "processo": "Numero do processo",
            }[valor],
            help="Escolha como voce quer montar a consulta.",
        )
        if modo_busca_sidebar == "classe":
            st.caption("Escolha a classe e, se quiser, refine por tema.")
        elif modo_busca_sidebar == "tema":
            st.caption("Pesquise um assunto direto no tribunal, como `plano de saude`.")
        else:
            st.caption("Busque um caso especifico pelo numero unico.")

        if "classe_codigo_sidebar" not in st.session_state:
            st.session_state["classe_codigo_sidebar"] = 12729
        classe_codigo = int(st.session_state.get("classe_codigo_sidebar", 12729) or 12729)
        if modo_busca_sidebar == "classe":
            classe_codigo = int(
                st.number_input(
                    "Classe processual (codigo CNJ)",
                    min_value=1,
                    step=1,
                    key="classe_codigo_sidebar",
                    help="Codigo CNJ do tipo de processo ou recurso.",
                )
            )
            render_codigo_sugestoes(tribunal_sigla)
            st.markdown(
                f"[Consultar codigos de classe (CNJ)]({CNJ_CLASSES_URL})"
            )

        numero_processo = ""
        if modo_busca_sidebar == "processo":
            numero_processo = st.text_input(
                "Numero do processo",
                key="numero_processo_sidebar",
                placeholder="Ex.: 50012345620248130024",
                help="Consulta o caso exato.",
            )

        aplicar_periodo = False
        data_inicio = None
        data_fim = None
        if modo_busca_sidebar != "processo":
            aplicar_periodo = st.checkbox(
                "Filtrar por periodo de ajuizamento",
                value=False,
                help="Limita a busca a um intervalo de ajuizamento.",
            )
        else:
            st.caption("Neste modo, o app ignora periodo e outros filtros para priorizar o caso exato.")
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
        usar_numero_processo_sidebar = modo_busca_sidebar == "processo"
        tema_cache_key = build_theme_suggestion_cache_key(
            tribunal_sigla=tribunal_sigla,
            classe_codigo=classe_codigo,
            estrutura_filtro=estrutura_filtro,
            data_inicio=data_inicio if aplicar_periodo else None,
            data_fim=data_fim if aplicar_periodo else None,
        )
        tema_sugestoes_key = "tema_sugestoes_df"
        tema_sugestoes_status_key = "tema_sugestoes_status"
        tema_sugestoes_cache_key = "tema_sugestoes_cache_key"
        if st.session_state.get(tema_sugestoes_cache_key) != tema_cache_key:
            st.session_state[tema_sugestoes_cache_key] = tema_cache_key
            st.session_state[tema_sugestoes_key] = pd.DataFrame(columns=["assunto", "quantidade"])
            st.session_state[tema_sugestoes_status_key] = ""
            st.session_state["tema_consulta_select"] = ""
            st.session_state["tema_consulta_text_fallback"] = ""
            st.session_state["tema_consulta_busca_local"] = ""

        tema_sugestoes_df = st.session_state.get(tema_sugestoes_key, pd.DataFrame(columns=["assunto", "quantidade"]))
        tema_sugestoes_status = str(st.session_state.get(tema_sugestoes_status_key, ""))
        tema_sugestoes_erro = ""

        tema_sugestoes = (
            tema_sugestoes_df["assunto"].dropna().astype(str).str.strip().tolist()
            if not tema_sugestoes_df.empty
            else []
        )
        tema_sugestoes = [tema for tema in tema_sugestoes if tema]
        tema_select_key = "tema_consulta_select"
        tema_text_key = "tema_consulta_text_fallback"
        tema_busca_key = "tema_consulta_busca_local"
        if tema_text_key not in st.session_state:
            st.session_state[tema_text_key] = ""
        if tema_busca_key not in st.session_state:
            st.session_state[tema_busca_key] = ""
        if tema_select_key not in st.session_state:
            st.session_state[tema_select_key] = ""
        tema_atual_sidebar = normalize_assunto_filtro(
            st.session_state.get(tema_text_key, "")
            or st.session_state.get(tema_select_key, "")
        )
        tema_consulta = tema_atual_sidebar
        busca_tema_direto_sidebar = modo_busca_sidebar == "tema"
        if usar_numero_processo_sidebar:
            pass
        elif modo_busca_sidebar == "tema":
            tema_consulta = normalize_assunto_filtro(
                st.text_input(
                    "Tema principal",
                    key=tema_text_key,
                    placeholder="Ex.: plano de saude, consumidor, servidor publico",
                    help="Use o assunto como filtro principal.",
                )
            )
            st.caption("O tema sera buscado em todo o tribunal selecionado.")
        else:
            tema_consulta = normalize_assunto_filtro(
                st.text_input(
                    "Tema (opcional)",
                    key=tema_text_key,
                    placeholder="Digite um tema para refinar a classe processual",
                    help="Use para afunilar a classe por assunto.",
                )
            )
            if st.button(
                "Mostrar temas da classe",
                key="carregar_temas_codigo_sigla",
                use_container_width=True,
            ):
                temas_da_consulta_atual = pd.DataFrame(columns=["assunto", "quantidade"])
                if current_query_can_seed_theme_suggestions(
                    classe_codigo=classe_codigo,
                    tribunal_sigla=tribunal_sigla,
                    estrutura_filtro=estrutura_filtro,
                    data_inicio=data_inicio if aplicar_periodo else None,
                    data_fim=data_fim if aplicar_periodo else None,
                ):
                    temas_da_consulta_atual = build_theme_suggestions_from_current_query()
                if not temas_da_consulta_atual.empty:
                    st.session_state[tema_sugestoes_key] = temas_da_consulta_atual
                    st.session_state[tema_sugestoes_status_key] = (
                        f"Usei a consulta atual para montar {format_int_br(len(temas_da_consulta_atual))} sugestoes."
                    )
                    st.rerun()
                with st.spinner("Carregando temas sugeridos..."):
                    try:
                        tema_sugestoes_df = fetch_theme_suggestions_dataframe(
                            api_key=api_key,
                            classe_codigo=int(classe_codigo),
                            url=build_url(tribunal_sigla),
                            tribunal_sigla=tribunal_sigla,
                            estrutura_filtro=estrutura_filtro,
                            data_inicio=data_inicio if aplicar_periodo else None,
                            data_fim=data_fim if aplicar_periodo else None,
                        )
                    except DataJudRequestError:
                        tema_sugestoes_erro = (
                            "Os temas sugeridos demoraram para carregar. "
                            "Voce ainda pode digitar o tema."
                        )
                        st.session_state[tema_sugestoes_status_key] = tema_sugestoes_erro
                    except Exception:
                        tema_sugestoes_erro = (
                            "A lista de temas nao ficou disponivel nesta tentativa."
                        )
                        st.session_state[tema_sugestoes_status_key] = tema_sugestoes_erro
                    else:
                        st.session_state[tema_sugestoes_key] = tema_sugestoes_df
                        if tema_sugestoes_df.empty:
                            st.session_state[tema_sugestoes_status_key] = (
                                "Nao encontrei temas sugeridos nesta amostra. "
                                "Voce ainda pode digitar o tema."
                            )
                        else:
                            st.session_state[tema_sugestoes_status_key] = (
                                f"Lista carregada com {format_int_br(len(tema_sugestoes_df))} sugestoes."
                            )
                        st.rerun()
            if tema_sugestoes_status:
                st.caption(tema_sugestoes_status)
            else:
                st.caption(
                    "Se quiser ajuda, carregue temas sugeridos desta classe."
                )
            if tema_sugestoes:
                busca_local = normalize_assunto_filtro(
                    st.text_input(
                        "Filtrar sugestoes de tema",
                        key=tema_busca_key,
                        placeholder="Filtre as sugestoes por palavra-chave",
                    )
                )
                temas_filtrados = [
                    tema for tema in tema_sugestoes
                    if busca_local.lower() in tema.lower()
                ] if busca_local else tema_sugestoes
                tema_options = [""] + temas_filtrados
                if tema_consulta and tema_consulta not in tema_options:
                    tema_options.append(tema_consulta)
                if st.session_state.get(tema_select_key) not in tema_options:
                    st.session_state[tema_select_key] = (
                        tema_consulta if tema_consulta in tema_options else ""
                    )
                st.selectbox(
                    "Usar tema sugerido",
                    options=tema_options,
                    key=tema_select_key,
                    format_func=lambda valor: "Nao usar sugestao" if not valor else valor,
                    help="Escolha uma sugestao para preencher o campo Tema.",
                    on_change=sync_tema_text_from_select,
                )
                if busca_local and not temas_filtrados:
                    st.caption("Nenhum tema sugerido bateu com essa busca.")
                else:
                    st.caption(
                        f"{format_int_br(len(tema_sugestoes))} temas encontrados em ate {format_int_br(THEME_SUGGESTION_SAMPLE_SIZE)} registros."
                    )
        if tema_consulta and not usar_numero_processo_sidebar:
            st.caption(
                "A quantidade passa a contar apenas processos com esse tema."
            )
        st.markdown("**3. Tamanho e velocidade**")
        st.caption(
            "No modo rapido, o app prioriza a resposta principal e pode reduzir leituras complementares."
        )
        modo_rapido = st.checkbox(
            "Modo rapido (recomendado)",
            value=True,
            help="Acelera a resposta.",
        )
        ampliar_historico = st.checkbox(
            "Ampliar historico mensal automaticamente (mais lento)",
            value=False,
            help="Tenta preencher melhor o grafico mensal.",
        )
        mostrar_graficos_avancados = st.checkbox(
            "Exibir graficos avancados (mais lento)",
            value=False,
            help="Mostra graficos extras.",
        )
        size = st.number_input("Quantidade da amostra", min_value=1, max_value=MAX_TOTAL_SIZE, value=700, step=100)
        if modo_busca_sidebar == "tema" and modo_rapido:
            st.caption(
                "Na busca por tema com modo rapido, o app respeita a quantidade escolhida e mantem uma leitura estrategica inicial mais leve para nao esconder os comparativos principais."
            )
        if size > MAX_PAGE_SIZE:
            st.info(
                "Acima de 10.000 registros, o app pagina automaticamente a consulta no DataJud. "
                "Isso pode deixar a resposta mais lenta."
            )
        auto_url = build_url(tribunal_sigla)
        url = auto_url
        st.caption(f"URL usada: {url}")
        executar = st.button("Buscar no DataJud", use_container_width=True)
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
                tema_consulta_limpo = "" if usar_numero_processo else normalize_assunto_filtro(tema_consulta)
                busca_tema_direto = bool(busca_tema_direto_sidebar and tema_consulta_limpo and not usar_numero_processo)
                classe_codigo_consulta = 0 if busca_tema_direto else int(classe_codigo)
                modo_consulta_base = "tema_direto" if busca_tema_direto else "classe_ou_processo"
                data_inicio_consulta = None if usar_numero_processo else data_inicio
                data_fim_consulta = None if usar_numero_processo else data_fim
                if modo_busca_sidebar == "processo" and not usar_numero_processo:
                    st.error("Preencha o numero do processo para usar esse modo de busca.")
                    return
                if busca_tema_direto_sidebar and not tema_consulta_limpo and not usar_numero_processo:
                    st.error("Preencha o campo Tema para usar a busca direta por tema no tribunal.")
                    return
                if usar_numero_processo and normalize_assunto_filtro(tema_consulta):
                    avisos_consulta.append(
                        "O filtro de tema/assunto foi ignorado porque a consulta por numero do processo prioriza o caso exato."
                    )
                elif busca_tema_direto:
                    avisos_consulta.append(
                        "Busca direta por tema ativa: o app ignorou o codigo da classe e pesquisou este tema no tribunal selecionado."
                    )
                size_efetivo = int(size)
                timeout_consulta = DATAJUD_TIMEOUT_SECONDS
                if busca_tema_direto:
                    timeout_consulta = THEME_DIRECT_TIMEOUT_SECONDS
                hits = fetch_hits(
                    api_key=api_key,
                    classe_codigo=classe_codigo_consulta,
                    size=size_efetivo,
                    url=url,
                    numero_processo=numero_processo,
                    assunto_nome=tema_consulta_limpo,
                    data_inicio=data_inicio_consulta,
                    data_fim=data_fim_consulta,
                    incluir_movimentos=not modo_rapido,
                    modo_consulta=modo_consulta_base,
                    timeout_seconds=timeout_consulta,
                )
                df_anpp = hits_to_dataframe(hits, processar_movimentos=not modo_rapido)
                if not usar_numero_processo:
                    df_anpp = filter_dataframe_by_estrutura(df_anpp, tribunal_sigla, estrutura_filtro)
                else:
                    df_anpp = add_estrutura_column(df_anpp, tribunal_sigla)
                top_100 = build_top_100(df_anpp)
                size_int = int(size_efetivo)
                mapa_size = 0
                decisao_size = 0
                top_codigos = pd.DataFrame()
                top_orgaos_sigla = pd.DataFrame()
                top_assuntos = pd.DataFrame()
                df_decisao = pd.DataFrame()
                qtd_mapa = 0
                qtd_decisao = 0

                if not usar_numero_processo:
                    if modo_rapido:
                        if busca_tema_direto:
                            decisao_size = min(size_int, THEME_DIRECT_FAST_DECISION_LIMIT)
                            avisos_consulta.append(
                                "Busca por tema em modo rapido: mantive uma leitura estrategica inicial mais leve para nao sumirem os comparativos principais. Se quiser aprofundar, a estrategia ainda pode ser reforcada depois."
                            )
                        elif size_int > FAST_COMPLEMENTARY_SKIP_THRESHOLD:
                            decisao_size = min(size_int, FAST_DECISION_SAMPLE_LIMIT)
                            mapa_size = min(size_int, FAST_MAP_SAMPLE_LIMIT)
                        else:
                            avisos_consulta.append(
                                "Busca simples em modo rapido: o app priorizou a resposta principal e pulou a leitura decisoria complementar e o mapa automatico da sigla."
                            )
                    else:
                        mapa_size = min(max(size_int, 2000), MAX_PAGE_SIZE)
                        decisao_size = min(max(size_int, 400), 1200)

                    if modo_rapido:
                        if decisao_size > 0:
                            try:
                                hits_decisao = fetch_hits(
                                    api_key=api_key,
                                    classe_codigo=classe_codigo_consulta,
                                    size=decisao_size,
                                    url=url,
                                    numero_processo="",
                                    assunto_nome=tema_consulta_limpo,
                                    data_inicio=data_inicio_consulta,
                                    data_fim=data_fim_consulta,
                                    incluir_movimentos=True,
                                    modo_consulta=modo_consulta_base,
                                    source_fields=DECISION_SOURCE_FIELDS,
                                )
                                df_decisao = hits_to_dataframe(hits_decisao, processar_movimentos=True)
                            except DataJudRequestError as exc:
                                avisos_consulta.append(
                                    "Nao consegui montar a leitura decisoria complementar nesta tentativa. "
                                    f"{exc}"
                                )
                                df_decisao = pd.DataFrame()
                    else:
                        if decisao_size > 0:
                            df_decisao = df_anpp.head(decisao_size).copy()

                    if not df_decisao.empty:
                        df_decisao = enrich_decision_proxy_dataframe(df_decisao)
                        df_decisao = filter_dataframe_by_estrutura(df_decisao, tribunal_sigla, estrutura_filtro)
                        df_decisao = add_comparison_columns(df_decisao)
                        qtd_decisao = len(df_decisao)

                    if busca_tema_direto and not df_anpp.empty:
                        top_codigos = top_codigos_dataframe(df_anpp)
                        top_orgaos_sigla = top_orgaos_julgadores_dataframe(df_anpp)
                        top_assuntos = top_assuntos_dataframe(df_anpp)
                        qtd_mapa = len(df_anpp)
                    elif mapa_size > 0:
                        try:
                            hits_mapa = fetch_hits(
                                api_key=api_key,
                                classe_codigo=classe_codigo_consulta,
                                size=mapa_size,
                                url=url,
                                numero_processo="",
                                assunto_nome=tema_consulta_limpo,
                                data_inicio=data_inicio_consulta,
                                data_fim=data_fim_consulta,
                                incluir_movimentos=False,
                                modo_consulta="mapa_tribunal",
                                timeout_seconds=timeout_consulta,
                            )
                            df_mapa = hits_to_dataframe(hits_mapa, processar_movimentos=False)
                            df_mapa = filter_dataframe_by_estrutura(df_mapa, tribunal_sigla, estrutura_filtro)
                            top_codigos = top_codigos_dataframe(df_mapa)
                            top_orgaos_sigla = top_orgaos_julgadores_dataframe(df_mapa)
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
                                classe_codigo=classe_codigo_consulta,
                                size=10000,
                                url=url,
                                numero_processo="",
                                assunto_nome=tema_consulta_limpo,
                                data_inicio=data_inicio_consulta,
                                data_fim=data_fim_consulta,
                                incluir_movimentos=False,
                                modo_consulta=modo_consulta_base,
                                timeout_seconds=timeout_consulta,
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
        st.session_state["top_orgaos_sigla"] = top_orgaos_sigla
        st.session_state["top_assuntos"] = top_assuntos
        st.session_state["df_decisao"] = df_decisao
        st.session_state["qtd_decisao"] = qtd_decisao
        st.session_state["usar_numero_processo"] = usar_numero_processo
        st.session_state["busca_tema_direto"] = bool(busca_tema_direto)
        st.session_state["estrutura_filtro"] = estrutura_filtro
        st.session_state["periodo_aplicado"] = format_periodo_aplicado(data_inicio_consulta, data_fim_consulta)
        st.session_state["periodo_ignorado_numero"] = bool(usar_numero_processo and aplicar_periodo)
        st.session_state["tema_consulta_aplicado"] = tema_consulta_limpo
        st.session_state["avisos_consulta"] = avisos_consulta
        st.session_state["last_query_context"] = {
            "classe_codigo": int(classe_codigo_consulta),
            "classe_codigo_referencia": int(classe_codigo),
            "url": url,
            "tribunal_sigla": tribunal_sigla,
            "estrutura_filtro": estrutura_filtro,
            "data_inicio_consulta": data_inicio_consulta,
            "data_fim_consulta": data_fim_consulta,
            "tema_consulta": tema_consulta_limpo,
            "query_size": int(size_int),
            "query_size_requested": int(size),
            "qtd_decisao": qtd_decisao,
            "usar_numero_processo": bool(usar_numero_processo),
            "busca_tema_direto": bool(busca_tema_direto),
        }
        st.session_state["derived_state"] = build_query_derived_state(
            df_anpp=df_anpp,
            df_mensal=df_mensal,
            top_100=top_100,
            top_codigos=top_codigos,
            top_orgaos_sigla=top_orgaos_sigla,
            top_assuntos=top_assuntos,
            df_decisao=df_decisao,
            qtd_mapa=qtd_mapa,
        )
        st.success(f"Consulta concluida em {elapsed:.1f}s. Registros: {len(df_anpp)}")

    if "df_anpp" not in st.session_state:
        st.info("Preencha os filtros e clique em 'Buscar no DataJud'. Comece com 1000 ou 2000 registros.")
        return

    df_anpp = st.session_state["df_anpp"]
    df_mensal = st.session_state.get("df_mensal", df_anpp)
    top_100 = st.session_state["top_100"]
    top_codigos = st.session_state.get("top_codigos", pd.DataFrame())
    top_orgaos_sigla = st.session_state.get("top_orgaos_sigla", pd.DataFrame())
    top_assuntos = st.session_state.get("top_assuntos", pd.DataFrame())
    df_decisao = st.session_state.get("df_decisao", pd.DataFrame())
    if isinstance(df_decisao, pd.DataFrame) and not df_decisao.empty and "comparativo_orgao" not in df_decisao.columns:
        df_decisao = add_comparison_columns(df_decisao)
        st.session_state["df_decisao"] = df_decisao
    qtd_mapa = int(st.session_state.get("qtd_mapa", 0) or 0)
    qtd_decisao = int(st.session_state.get("qtd_decisao", 0) or 0)
    usar_numero_processo = bool(st.session_state.get("usar_numero_processo", False))
    busca_tema_direto = bool(st.session_state.get("busca_tema_direto", False))
    estrutura_filtro = str(st.session_state.get("estrutura_filtro", "Todos"))
    periodo_aplicado = str(st.session_state.get("periodo_aplicado", ""))
    periodo_ignorado_numero = bool(st.session_state.get("periodo_ignorado_numero", False))
    tema_consulta_aplicado = normalize_assunto_filtro(st.session_state.get("tema_consulta_aplicado", ""))
    avisos_consulta = st.session_state.get("avisos_consulta", [])
    last_query_context = st.session_state.get("last_query_context", {})
    derived_state = st.session_state.get("derived_state")
    derived_state_required_keys = {
        "df_view",
        "top_100_df",
        "top_orgaos_df",
        "top_comarcas_df",
        "top_classes_df",
        "sample_insights",
        "map_insights",
        "assuntos_distintos",
        "total_assuntos",
        "temas_decisao",
        "temas_overview",
    }
    if not isinstance(derived_state, dict) or not derived_state_required_keys.issubset(derived_state.keys()):
        derived_state = build_query_derived_state(
            df_anpp=df_anpp,
            df_mensal=df_mensal,
            top_100=top_100,
            top_codigos=top_codigos,
            top_orgaos_sigla=top_orgaos_sigla,
            top_assuntos=top_assuntos,
            df_decisao=df_decisao,
            qtd_mapa=qtd_mapa,
        )
        st.session_state["derived_state"] = derived_state
    df_view = derived_state["df_view"]
    top_100_df = derived_state["top_100_df"]
    top_orgaos_df = derived_state["top_orgaos_df"]
    top_classes_df = derived_state.get("top_classes_df", top_classes_display_dataframe(df_anpp))
    sample_insights = derived_state["sample_insights"]
    map_insights = derived_state["map_insights"]
    tema_insights: list[str] = []
    tema_escolhido = ""
    assuntos_distintos = derived_state["assuntos_distintos"]
    total_assuntos = int(derived_state.get("total_assuntos", 0) or 0)

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
    if tema_consulta_aplicado and not usar_numero_processo:
        st.caption(f"Filtro tematico aplicado na busca: `{tema_consulta_aplicado}`.")
    if busca_tema_direto and not usar_numero_processo:
        st.caption("Modo de busca ativa: tema direto no tribunal, sem limitar pelo codigo da classe.")
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
    elif not isinstance(df_decisao, pd.DataFrame) or df_decisao.empty:
        st.info(
            "A consulta principal foi concluida, mas a leitura decisoria complementar ainda nao esta carregada nesta amostra."
        )
        pode_ampliar_leitura = (
            isinstance(last_query_context, dict)
            and (
                bool(last_query_context.get("busca_tema_direto", False))
                or bool(last_query_context.get("classe_codigo"))
            )
            and bool(last_query_context.get("url"))
            and bool(last_query_context.get("tribunal_sigla"))
            and not bool(last_query_context.get("usar_numero_processo", False))
        )
        target_size = strategy_reload_target_size(
            last_query_context.get("query_size", 0),
            last_query_context.get("qtd_decisao", qtd_decisao),
        )
        st.caption(
            "Sem essa camada complementar, a parte de temas, favorabilidade e tempo pode ficar vazia no modo rapido."
        )
        if pode_ampliar_leitura:
            if st.button(
                f"Carregar leitura decisoria complementar (ate {format_int_br(target_size)} registros)",
                key="carregar_leitura_decisoria_complementar",
            ):
                with st.spinner("Carregando leitura decisoria complementar..."):
                    try:
                        df_decisao_carregado, decision_size = fetch_strategy_decision_dataframe(
                            api_key=api_key,
                            query_context=last_query_context,
                            target_size=target_size,
                        )
                    except DataJudRequestError as exc:
                        st.error(
                            "Nao consegui carregar a leitura decisoria complementar nesta tentativa. "
                            f"{exc}"
                        )
                    except Exception as exc:
                        st.error(str(exc))
                    else:
                        if df_decisao_carregado.empty:
                            st.warning(
                                "A leitura decisoria complementar foi consultada, mas voltou sem base aproveitavel para este filtro estrutural."
                            )
                        else:
                            replace_decision_state_in_session(
                                df_decisao_carregado,
                                target_size=decision_size,
                                aviso=(
                                    "Leitura decisoria complementar carregada sob demanda para liberar os comparativos "
                                    f"de tema com ate {format_int_br(decision_size)} registros."
                                ),
                            )
                            st.rerun()
    elif isinstance(df_decisao, pd.DataFrame) and not df_decisao.empty:
        temas_decisao = derived_state["temas_decisao"]
        temas_overview = derived_state["temas_overview"]
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
            tema_options = ["Todos os temas"] + tema_opcoes
            tema_select_key = "tema_para_analisar"
            tema_focado_automaticamente = bool(
                tema_consulta_aplicado and tema_consulta_aplicado in tema_options
            )
            tema_prefill = (
                tema_consulta_aplicado
                if tema_consulta_aplicado and tema_consulta_aplicado in tema_options
                else "Todos os temas"
            )
            tema_query_signature = (
                f"{tema_consulta_aplicado}|{qtd_decisao}|{len(tema_opcoes)}|"
                f"{int(bool(busca_tema_direto))}"
            )
            if st.session_state.get("tema_query_signature") != tema_query_signature:
                st.session_state["tema_query_signature"] = tema_query_signature
                st.session_state[tema_select_key] = tema_prefill
            elif st.session_state.get(tema_select_key) not in tema_options:
                st.session_state[tema_select_key] = tema_prefill
            if tema_focado_automaticamente:
                st.session_state[tema_select_key] = tema_prefill
                tema_escolhido = tema_prefill
                st.markdown(f"**Tema principal da leitura:** `{tema_escolhido}`")
            else:
                tema_escolhido = st.selectbox(
                    "Tema para analisar",
                    options=tema_options,
                    key=tema_select_key,
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
            if tema_consulta_aplicado and tema_escolhido == tema_consulta_aplicado:
                st.success(
                    f"Analise principal focada no tema pesquisado: `{tema_escolhido}`. "
                    "A visao geral dos outros temas continua disponivel logo abaixo como apoio."
                )
            if not temas_overview.empty:
                with st.expander("Ver visao geral dos temas da amostra", expanded=False):
                    st.caption(
                        "Use este quadro como contexto. A leitura principal desta consulta continua focada no tema selecionado acima."
                    )
                    st.dataframe(temas_overview, use_container_width=True, height=320)
            df_tema_decisao = filter_dataframe_by_tema(df_decisao, tema_escolhido)
            desfechos_tema = decision_outcomes_dataframe(df_tema_decisao)
            movimentos_tema = decision_movements_dataframe(df_tema_decisao)
            orgaos_tema_base = decision_by_orgao_dataframe(df_tema_decisao)
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
            comparison_state_by_dimension = build_comparison_dimension_state(df_tema_decisao)
            dimensao_recomendada, dimensao_recomendada_state = recommended_comparison_dimension(
                comparison_state_by_dimension
            )
            if int(dimensao_recomendada_state.get("score", 0) or 0) > 0:
                st.caption(
                    f"Recorte recomendado agora: {COMPARISON_DIMENSIONS[dimensao_recomendada]['label']}. "
                    f"Hoje ele e o recorte com mais base comparavel, com "
                    f"{format_int_br(dimensao_recomendada_state.get('decisoes_uteis', 0))} decisoes uteis e "
                    f"{format_int_br(dimensao_recomendada_state.get('grupos_favorabilidade', 0))} itens que ja podem ser comparados."
                )
            else:
                st.caption(
                    "O app tenta sugerir o recorte com mais massa critica. Se todos vierem fracos, a comparacao ainda fica exploratoria."
                )
            dimensao_comparativa = st.radio(
                "Recorte comparativo da estrategia",
                options=list(COMPARISON_DIMENSIONS.keys()),
                format_func=lambda key: COMPARISON_DIMENSIONS[key]["label"],
                index=list(COMPARISON_DIMENSIONS.keys()).index(dimensao_recomendada),
                horizontal=True,
                key=f"comparacao_tema_{tema_escolhido}",
            )
            dimensao_config = COMPARISON_DIMENSIONS[dimensao_comparativa]
            rotulo_comparativo = str(dimensao_config["label"])
            eixo_comparativo = str(dimensao_config["axis_label"])
            coluna_tabela_comparativa = str(dimensao_config["table_label"])
            plural_comparativo = str(dimensao_config["plural_label"])
            comparison_state = comparison_state_by_dimension.get(dimensao_comparativa, {})
            orgaos_tema = comparison_state.get("orgaos_tema", pd.DataFrame())
            mix_orgaos_tema = comparison_state.get("mix_orgaos_tema", pd.DataFrame())
            mix_profile_info = outcome_mix_profile_summary(mix_orgaos_tema)
            favorabilidade_orgaos = comparison_state.get("favorabilidade_orgaos", pd.DataFrame())
            favorabilidade_minima_utilizada = int(
                comparison_state.get("favorabilidade_minima", 5) or 5
            )
            tempo_orgaos = comparison_state.get("tempo_orgaos", pd.DataFrame())
            tempo_minimo_utilizado = int(comparison_state.get("tempo_minimo", 3) or 3)
            estabilidade_orgaos = comparison_state.get("estabilidade_orgaos", pd.DataFrame())
            if (
                dimensao_comparativa != dimensao_recomendada
                and int(dimensao_recomendada_state.get("score", 0) or 0)
                > int(comparison_state.get("score", 0) or 0)
            ):
                st.info(
                    f"Para este tema, {COMPARISON_DIMENSIONS[dimensao_recomendada]['label'].lower()} tende a mostrar mais sinal util do que {rotulo_comparativo.lower()}."
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
                recorte_label_plural=plural_comparativo,
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
            mediana_dias = format_duration_label(dias_decisao.median()) if not dias_decisao.empty else "-"
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
            mix_orgaos_tema_view = mix_orgaos_tema.rename(
                columns={"orgao_julgador": coluna_tabela_comparativa}
            )
            orgaos_tema_view = orgaos_tema.rename(
                columns={"orgao_julgador": coluna_tabela_comparativa}
            )
            if "mediana_dias" in orgaos_tema_view.columns:
                orgaos_tema_view["mediana_ate_primeiro_desfecho"] = orgaos_tema_view["mediana_dias"].apply(
                    format_duration_label
                )
                orgaos_tema_view = orgaos_tema_view.drop(columns=["mediana_dias"])
            favorabilidade_orgaos_view = favorabilidade_orgaos.rename(
                columns={"orgao_julgador": coluna_tabela_comparativa}
            )
            ranking_favoraveis_view = ranking_favoraveis.rename(
                columns={"orgao_julgador": coluna_tabela_comparativa}
            )
            ranking_restritivos_view = ranking_restritivos.rename(
                columns={"orgao_julgador": coluna_tabela_comparativa}
            )
            tempo_orgaos_view = tempo_orgaos.rename(
                columns={"orgao_julgador": coluna_tabela_comparativa}
            )
            estabilidade_orgaos_view = estabilidade_orgaos.rename(
                columns={"orgao_julgador": coluna_tabela_comparativa}
            )
            tempo_orgaos_fallback = pd.DataFrame(columns=["orgao_julgador", "processos_com_tempo", "mediana_dias", "p75_dias"])
            if tempo_orgaos.empty and not orgaos_tema.empty and "mediana_dias" in orgaos_tema.columns:
                tempo_orgaos_fallback = (
                    orgaos_tema[["orgao_julgador", "processos_tema", "mediana_dias"]]
                    .copy()
                    .rename(columns={"processos_tema": "processos_com_tempo"})
                )
                tempo_orgaos_fallback["p75_dias"] = pd.NA
                tempo_orgaos_fallback = tempo_orgaos_fallback[
                    tempo_orgaos_fallback["mediana_dias"].notna()
                ].reset_index(drop=True)
            tempo_orgaos_plot = tempo_orgaos if not tempo_orgaos.empty else tempo_orgaos_fallback
            tempo_orgaos_plot_view = tempo_orgaos_plot.rename(
                columns={"orgao_julgador": coluna_tabela_comparativa}
            )
            if "mediana_dias" in tempo_orgaos_plot_view.columns:
                tempo_orgaos_plot_view["mediana_ate_primeiro_desfecho"] = tempo_orgaos_plot_view["mediana_dias"].apply(
                    format_duration_label
                )
            if "p75_dias" in tempo_orgaos_plot_view.columns:
                tempo_orgaos_plot_view["p75_ate_primeiro_desfecho"] = tempo_orgaos_plot_view["p75_dias"].apply(
                    format_duration_label
                )
            tempo_orgaos_plot_view = tempo_orgaos_plot_view.drop(
                columns=["mediana_dias", "p75_dias"],
                errors="ignore",
            )

            d1, d2, d3, d4 = st.columns(4)
            render_theme_metric_card(d1, "Processos na amostra", f"{total_tema:,}".replace(",", "."))
            render_theme_metric_card(d2, "Cobertura de desfecho", f"{cobertura:.1f}%")
            render_theme_metric_card(d3, "Desfecho predominante", desfecho_predominante_card)
            render_theme_metric_card(d4, "Cobertura de movimento final", f"{cobertura_movimento:.1f}%")
            e1, e2, e3, e4 = st.columns(4)
            render_theme_metric_card(e1, "Confianca da leitura", forca_tema)
            render_theme_metric_card(e2, "Favorabilidade estimada", leitura_favorabilidade, delta_favorabilidade)
            render_theme_metric_card(e3, "Repeticao do padrao", perfil_estabilidade, delta_estabilidade)
            render_theme_metric_card(e4, "Mudanca recente", mudanca_label, delta_mudanca)
            resumo_estatistico_tema = (
                f"Neste tema, o app olhou {format_int_br(total_tema)} processos da amostra atual. "
                f"Em {cobertura_movimento:.1f}% deles encontrou algum movimento final e em {cobertura:.1f}% conseguiu classificar um desfecho automaticamente. "
                f"O desfecho mais comum foi `{desfecho_predominante_card}`."
            )
            if leitura_favorabilidade == "Sem base":
                resumo_estatistico_tema += (
                    " A favorabilidade ficou sem base porque ainda faltaram desfechos uteis pro/contra, "
                    "ou porque os sinais vieram muito neutros ou processuais."
                )
            else:
                resumo_estatistico_tema += (
                    f" Com isso, a tendencia estimada do tema ficou em `{leitura_favorabilidade}`."
                )
            st.info(resumo_estatistico_tema)
            with st.expander("Como ler estes indicadores", expanded=False):
                st.markdown(
                    f"- `Processos na amostra`: quantos processos da consulta atual contem este tema. Aqui: {format_int_br(total_tema)}."
                )
                st.markdown(
                    f"- `Cobertura de movimento final`: percentual de processos do tema em que o app encontrou algum movimento final util. Aqui: {cobertura_movimento:.1f}%."
                )
                st.markdown(
                    f"- `Cobertura de desfecho`: percentual de processos do tema em que o app conseguiu classificar o desfecho automaticamente. Aqui: {cobertura:.1f}%."
                )
                st.markdown(
                    "- `Desfecho predominante`: o desfecho classificado que mais se repetiu entre os casos lidos."
                )
                st.markdown(
                    "- `Confianca da leitura`: resume se a base esta mais forte ou mais fraca, olhando tamanho da amostra e cobertura de desfechos."
                )
                st.markdown(
                    "- `Favorabilidade estimada`: tenta medir se o tema pende mais para sinais favoraveis ou restritivos. Ela so usa desfechos uteis para pro/contra; sinais neutros ou processuais pesam menos ou podem nao entrar."
                )
                st.markdown(
                    "- `Repeticao do padrao`: mostra se o tema tende a repetir o mesmo tipo de desfecho ou se oscila muito."
                )
                st.markdown(
                    "- `Mudanca recente`: compara os meses mais recentes com a janela anterior para ver se o comportamento mudou."
                )
                st.caption(
                    "Quando aparecer `Sem base`, isso nao quer dizer ausencia total de dados. Significa que ainda nao ha massa util suficiente para aquela metrica especifica."
                )
            tema_insights = build_decision_theme_insights(
                tema_escolhido,
                total_tema,
                total_com_desfecho,
                desfechos_tema,
                movimentos_tema,
                orgaos_tema_base,
                forca_tema,
                concentracao_tema,
                tendencia_tema,
                favorabilidade_tema,
                estabilidade_tema,
                mudanca_padrao,
                alertas_tema,
            )
            tema_tabs = st.tabs(["Resumo do tema", "Leituras", "Recortes", "Estrategia", "Contexto do tema"])
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
                    st.markdown(f"- Mediana ate o primeiro desfecho identificado: {mediana_dias}")
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
                st.markdown(f"**Como {rotulo_comparativo.lower()} aparece neste tema**")
                st.caption(
                    f"Recorte ativo: {rotulo_comparativo}. O comparativo abaixo resume volume, cobertura e sinal principal neste nivel."
                )
                if not mix_orgaos_tema.empty:
                    if bool(mix_profile_info.get("uniforme", False)):
                        st.markdown(f"**Base classificada por {eixo_comparativo.lower()}**")
                        desfecho_uniforme = str(mix_profile_info.get("desfecho_dominante", "")).strip()
                        if desfecho_uniforme:
                            st.caption(
                                f"Como praticamente todos os itens deste recorte caem no mesmo desfecho (`{desfecho_uniforme}`), o app mostra onde ha mais volume e cobertura de leitura em vez de repetir barras identicas."
                            )
                        else:
                            st.caption(
                                f"Como o padrao classificado ficou muito uniforme neste recorte, o app mostra volume e cobertura de leitura em vez de repetir barras identicas."
                            )
                    else:
                        st.markdown(f"**Desfecho por {eixo_comparativo.lower()}**")
                        st.caption(
                            f"Compara a composicao dos desfechos classificados entre os principais itens de {rotulo_comparativo.lower()}."
                        )
                    col_mix_chart, col_mix_table = st.columns(2)
                    with col_mix_chart:
                        if bool(mix_profile_info.get("uniforme", False)):
                            st.pyplot(
                                fig_base_classificada_por_orgao(
                                    orgaos_tema,
                                    titulo=f"Base classificada por {eixo_comparativo.lower()}",
                                    eixo_label=eixo_comparativo,
                                ),
                                clear_figure=True,
                            )
                        else:
                            st.pyplot(
                                fig_desfechos_por_orgao(
                                    mix_orgaos_tema,
                                    titulo=f"Desfecho por {eixo_comparativo.lower()}",
                                    eixo_label=eixo_comparativo,
                                ),
                                clear_figure=True,
                            )
                    with col_mix_table:
                        st.dataframe(
                            orgaos_tema_view if bool(mix_profile_info.get("uniforme", False)) else mix_orgaos_tema_view,
                            use_container_width=True,
                            height=360,
                        )
                else:
                    st.info(
                        f"Ainda nao ha desfechos classificados suficientes para comparar {rotulo_comparativo.lower()} neste tema."
                    )
                st.markdown(f"**Resumo por {eixo_comparativo.lower()}**")
                if not orgaos_tema.empty:
                    st.dataframe(orgaos_tema_view, use_container_width=True, height=320)
                else:
                    st.info(f"Nao encontrei dados suficientes por {rotulo_comparativo.lower()} neste tema.")
                st.markdown(f"**Taxa de desfecho por {eixo_comparativo.lower()}**")
                st.caption(
                    f"Mostra, por {eixo_comparativo.lower()}, a proporcao estimada de sinais favoraveis, desfavoraveis e mistos entre as decisoes uteis do tema."
                )
                if not favorabilidade_orgaos.empty and favorabilidade_minima_utilizada < 5:
                    st.caption(
                        f"Leitura expandida: para nao esconder o comparativo, o app aceitou {rotulo_comparativo.lower()} com pelo menos {favorabilidade_minima_utilizada} decisoes uteis. Use como sinal exploratorio."
                    )
                if not favorabilidade_orgaos.empty:
                    st.dataframe(
                        favorabilidade_orgaos_view.head(12),
                        use_container_width=True,
                        height=320,
                    )
                else:
                    st.info(
                        f"Ainda nao ha base util suficiente para medir favorabilidade estimada por {rotulo_comparativo.lower()} neste tema."
                    )
            with tema_tabs[3]:
                st.caption(
                    "Esta aba tenta traduzir o tema em sinais praticos. Primeiro veja a leitura principal; depois use os detalhes para confirmar."
                )
                decisoes_uteis_tema = int(favorabilidade_tema.get("decisoes_uteis", 0) or 0)
                total_classificados_tema = int(favorabilidade_tema.get("total_classificados", 0) or 0)
                base_util_estrategica = (
                    f"{format_int_br(decisoes_uteis_tema)} uteis / {format_int_br(total_classificados_tema)} classificadas"
                    if total_classificados_tema > 0
                    else "Sem base"
                )
                tempo_valores_plot = (
                    pd.to_numeric(tempo_orgaos_plot["mediana_dias"], errors="coerce").dropna()
                    if not tempo_orgaos_plot.empty and "mediana_dias" in tempo_orgaos_plot.columns
                    else pd.Series(dtype="float64")
                )
                max_tempo_dias = float(tempo_valores_plot.max()) if not tempo_valores_plot.empty else None
                if max_tempo_dias is None:
                    leitura_tempo_label = "Sem base"
                elif max_tempo_dias <= 0:
                    leitura_tempo_label = "No mesmo dia"
                elif max_tempo_dias < 1:
                    leitura_tempo_label = "Em horas"
                else:
                    leitura_tempo_label = "Em dias"

                if not favorabilidade_orgaos.empty:
                    leitura_principal_label = "Favorabilidade"
                    leitura_principal_texto = (
                        f"Ja ha base para comparar {rotulo_comparativo.lower()} por sinal mais favoravel ou mais restritivo."
                    )
                    leitura_principal_delta = (
                        f"Minimo usado: {favorabilidade_minima_utilizada} decisoes uteis"
                        if favorabilidade_minima_utilizada < 5
                        else None
                    )
                elif not mix_orgaos_tema.empty and not bool(mix_profile_info.get("uniforme", False)):
                    leitura_principal_label = "Composicao"
                    leitura_principal_texto = (
                        f"Ainda nao ha base forte para pro/contra. Comece pela composicao dos desfechos por {rotulo_comparativo.lower()}."
                    )
                    leitura_principal_delta = None
                elif not orgaos_tema.empty:
                    leitura_principal_label = "Cobertura"
                    leitura_principal_texto = (
                        f"O tema ficou mais uniforme neste recorte. Leia volume e cobertura por {rotulo_comparativo.lower()} antes de concluir favorabilidade."
                    )
                    leitura_principal_delta = None
                else:
                    leitura_principal_label = "Exploratoria"
                    leitura_principal_texto = (
                        "A base ainda esta curta para uma leitura comparativa mais firme."
                    )
                    leitura_principal_delta = None

                mudanca_card_label = (
                    mudanca_label
                    if int(mudanca_padrao.get("janela_meses", 0) or 0) > 0
                    else "Sem serie"
                )

                estrategia_fraca = (
                    favorabilidade_orgaos.empty
                    or tempo_orgaos.empty
                    or decisoes_uteis_tema < 10
                )
                strategy_target_size = strategy_reload_target_size(
                    last_query_context.get("query_size", 0),
                    last_query_context.get("qtd_decisao", qtd_decisao),
                )
                pode_reforcar_estrategia = (
                    isinstance(last_query_context, dict)
                    and (
                        bool(last_query_context.get("busca_tema_direto", False))
                        or bool(last_query_context.get("classe_codigo"))
                    )
                    and bool(last_query_context.get("url"))
                    and bool(last_query_context.get("tribunal_sigla"))
                    and not bool(last_query_context.get("usar_numero_processo", False))
                    and strategy_target_size > max(qtd_decisao, 0)
                )

                s1, s2, s3, s4 = st.columns(4)
                render_theme_metric_card(s1, "Base util", base_util_estrategica)
                render_theme_metric_card(s2, "Leitura principal", leitura_principal_label, leitura_principal_delta)
                render_theme_metric_card(s3, "Mudanca recente", mudanca_card_label)
                render_theme_metric_card(s4, "Tempo", leitura_tempo_label)

                guia_col, cuidado_col = st.columns([1.15, 0.85])
                with guia_col:
                    st.markdown("**1. O que olhar primeiro**")
                    if not favorabilidade_orgaos.empty:
                        st.success(leitura_principal_texto)
                    elif not orgaos_tema.empty:
                        st.info(leitura_principal_texto)
                    else:
                        st.warning(leitura_principal_texto)

                    st.markdown("**2. Painel visual**")
                    if not favorabilidade_orgaos.empty:
                        st.caption(
                            f"Compare o sinal mais favoravel ou mais restritivo entre itens de {rotulo_comparativo.lower()}."
                        )
                        st.pyplot(
                            fig_favorabilidade_por_orgao(
                                favorabilidade_orgaos.head(10),
                                titulo=f"Indice de favorabilidade por {eixo_comparativo.lower()}",
                                eixo_label=eixo_comparativo,
                            ),
                            clear_figure=True,
                        )
                    elif not mix_orgaos_tema.empty and not bool(mix_profile_info.get("uniforme", False)):
                        st.caption(
                            "Como a base pro/contra ainda esta curta, o painel mostra a distribuicao dos desfechos classificados."
                        )
                        st.pyplot(
                            fig_desfechos_por_orgao(
                                mix_orgaos_tema.head(10),
                                titulo=f"Composicao dos desfechos por {eixo_comparativo.lower()}",
                                eixo_label=eixo_comparativo,
                            ),
                            clear_figure=True,
                        )
                    elif not orgaos_tema.empty:
                        desfecho_uniforme = str(mix_profile_info.get("desfecho_dominante", "")).strip()
                        if desfecho_uniforme:
                            st.caption(
                                f"O padrao ficou concentrado em `{desfecho_uniforme}`. Por isso, o painel destaca volume e cobertura."
                            )
                        else:
                            st.caption(
                                "Como o padrao ficou uniforme, o painel destaca volume e cobertura por recorte."
                            )
                        st.pyplot(
                            fig_base_classificada_por_orgao(
                                orgaos_tema,
                                titulo=f"Base classificada por {eixo_comparativo.lower()}",
                                eixo_label=eixo_comparativo,
                            ),
                            clear_figure=True,
                        )
                    else:
                        st.info(
                            f"Ainda nao encontrei base suficiente para montar um painel comparativo por {rotulo_comparativo.lower()}."
                        )
                with cuidado_col:
                    st.markdown("**Leitura rapida**")
                    st.markdown(f"- Base util: {base_util_estrategica}")
                    st.markdown(f"- Recorte ativo: {rotulo_comparativo}")
                    st.markdown(f"- Desfecho predominante: {desfecho_predominante_card}")
                    if total_classificados_tema > 0:
                        st.markdown(
                            f"- Neutro/processual: {favorabilidade_tema['neutro_pct']:.1f}% dos classificados"
                        )
                    if dimensao_comparativa != dimensao_recomendada and int(dimensao_recomendada_state.get("score", 0) or 0) > 0:
                        st.markdown(
                            f"- Recorte com mais sinal agora: {COMPARISON_DIMENSIONS[dimensao_recomendada]['label']}"
                        )

                    st.markdown("**Mudanca recente**")
                    if int(mudanca_padrao.get("janela_meses", 0) or 0) > 0:
                        st.markdown(f"- Leitura principal: {mudanca_label}")
                        st.markdown(
                            f"- Janela recente: {', '.join(mudanca_padrao.get('meses_recentes', [])) or 'sem base'}"
                        )
                        st.markdown(
                            f"- Janela anterior: {', '.join(mudanca_padrao.get('meses_anteriores', [])) or 'sem base'}"
                        )
                        if mudanca_padrao.get("delta_indice") is not None:
                            st.markdown(
                                f"- Variacao do indice: {float(mudanca_padrao['delta_indice']):+.1f}"
                            )
                    else:
                        st.markdown("- Ainda nao ha meses suficientes para medir mudanca de padrao.")

                    st.markdown("**Cuidado principal**")
                    if alertas_tema:
                        st.warning(alertas_tema[0])
                        if len(alertas_tema) > 1:
                            st.caption(f"Ha mais {len(alertas_tema) - 1} alerta(s) nos detalhes abaixo.")
                    elif estrategia_fraca:
                        st.info(
                            "A base ainda esta curta. Prefira cobertura, volume e estabilidade antes de concluir favorabilidade."
                        )
                    else:
                        st.success("A leitura veio com base melhor para comparacao.")

                    if pode_reforcar_estrategia:
                        st.markdown("**Se quiser destravar mais base**")
                        if st.button(
                            f"Reforcar leitura (ate {format_int_br(strategy_target_size)})",
                            key=f"reforcar_estrategia_{tema_escolhido}",
                            use_container_width=True,
                        ):
                            with st.spinner("Ampliando base estrategica deste tema..."):
                                try:
                                    df_decisao_reforcado, decision_size = fetch_strategy_decision_dataframe(
                                        api_key=api_key,
                                        query_context=last_query_context,
                                        target_size=strategy_target_size,
                                    )
                                except DataJudRequestError as exc:
                                    st.error(
                                        "Nao consegui ampliar a base estrategica nesta tentativa. "
                                        f"{exc}"
                                    )
                                except Exception as exc:
                                    st.error(str(exc))
                                else:
                                    if df_decisao_reforcado.empty:
                                        st.warning(
                                            "A ampliacao foi executada, mas ainda nao voltou base estrategica suficiente para este filtro."
                                        )
                                    else:
                                        replace_decision_state_in_session(
                                            df_decisao_reforcado,
                                            target_size=decision_size,
                                            aviso=(
                                                "Base estrategica ampliada sob demanda para melhorar comparativos de favorabilidade "
                                                f"e tempo com ate {format_int_br(decision_size)} registros."
                                            ),
                                        )
                                        st.success(
                                            "Base estrategica atualizada. Mantive o tema selecionado para voce continuar daqui."
                                        )
                                        st.rerun()

                with st.expander(f"Ver base por {rotulo_comparativo.lower()}", expanded=False):
                    if not orgaos_tema.empty:
                        st.caption(
                            f"Esta tabela ajuda a entender volume, cobertura e mediana de tempo no recorte ativo."
                        )
                        st.dataframe(orgaos_tema_view, use_container_width=True, height=320)
                    else:
                        st.info(f"Nao encontrei dados suficientes por {rotulo_comparativo.lower()} neste tema.")

                with st.expander("Ver rankings comparativos", expanded=not favorabilidade_orgaos.empty):
                    col_rank1, col_rank2 = st.columns(2)
                    with col_rank1:
                        st.markdown(f"**{rotulo_comparativo} mais favoraveis**")
                        if not ranking_favoraveis.empty:
                            st.dataframe(ranking_favoraveis_view, use_container_width=True, height=260)
                        else:
                            st.info(
                                f"Sem base suficiente para ranquear {rotulo_comparativo.lower()} mais favoraveis neste tema."
                            )
                    with col_rank2:
                        st.markdown(f"**{rotulo_comparativo} mais restritivos**")
                        if not ranking_restritivos.empty:
                            st.dataframe(ranking_restritivos_view, use_container_width=True, height=260)
                        else:
                            st.info(
                                f"Sem base suficiente para ranquear {rotulo_comparativo.lower()} mais restritivos neste tema."
                            )

                with st.expander("Ver tempo e estabilidade", expanded=not tempo_orgaos_plot.empty):
                    col_tempo_chart, col_tempo_table = st.columns(2)
                    with col_tempo_chart:
                        st.markdown(f"**Tempo mediano ate o primeiro desfecho identificado por {eixo_comparativo.lower()}**")
                        st.caption(
                            "Este tempo e um proxy: o app mede do ajuizamento ate o primeiro desfecho que conseguiu identificar nos movimentos, e nao necessariamente ate o fim completo do processo."
                        )
                        if not tempo_orgaos.empty and tempo_minimo_utilizado < 3:
                            st.caption(
                                f"Base reduzida para exibicao: minimo de {tempo_minimo_utilizado} processo(s) com tempo por item deste recorte."
                            )
                        if tempo_orgaos.empty and not tempo_orgaos_fallback.empty:
                            st.caption(
                                "Leitura exploratoria: usei a mediana de tempo disponivel no resumo por recorte, mesmo sem massa suficiente para o comparativo completo."
                            )
                        if max_tempo_dias is not None:
                            if max_tempo_dias <= 0:
                                st.caption(
                                    "Nesta amostra, os movimentos classificados caem no mesmo dia do ajuizamento, entao o comparativo de tempo fica pouco informativo."
                                )
                            elif max_tempo_dias < 1:
                                st.caption(
                                    "Os tempos medianos deste tema ficaram abaixo de 1 dia. O grafico converte automaticamente para horas para evitar zeros aparentes."
                                )
                        st.pyplot(
                            fig_tempo_por_orgao(
                                tempo_orgaos_plot.head(10),
                                titulo=f"Tempo mediano ate o primeiro desfecho identificado por {eixo_comparativo.lower()}",
                                eixo_label=eixo_comparativo,
                            ),
                            clear_figure=True,
                        )
                        if not tempo_orgaos_plot.empty:
                            st.dataframe(tempo_orgaos_plot_view.head(12), use_container_width=True, height=260)
                        else:
                            st.info(
                                f"Sem base suficiente para comparar esse tempo por {rotulo_comparativo.lower()}."
                            )
                    with col_tempo_table:
                        st.markdown(f"**Estabilidade decisoria por {eixo_comparativo.lower()}**")
                        if not estabilidade_orgaos.empty:
                            st.dataframe(estabilidade_orgaos_view.head(12), use_container_width=True, height=320)
                        else:
                            st.info(
                                f"Sem base suficiente para medir estabilidade decisoria por {rotulo_comparativo.lower()}."
                            )
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
        st.subheader("Classes com mais processos")
        st.caption("Mostra as classes processuais mais frequentes na amostra. Nomes longos aparecem resumidos para facilitar a leitura.")
        if isinstance(top_classes_df, pd.DataFrame) and not top_classes_df.empty:
            st.dataframe(top_classes_df, use_container_width=True, height=320)
        else:
            st.info("Sem classes suficientes para montar esse ranking na amostra atual.")

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
        col_codigos, col_orgaos, col_assuntos = st.columns(3)
        with col_codigos:
            st.markdown("**Top 10 codigos**")
            st.dataframe(top_codigos, use_container_width=True, height=320)
        with col_orgaos:
            st.markdown("**Top 10 orgaos julgadores**")
            if isinstance(top_orgaos_sigla, pd.DataFrame) and not top_orgaos_sigla.empty:
                st.dataframe(top_orgaos_sigla, use_container_width=True, height=320)
            else:
                st.info("Sem base suficiente para ranquear orgaos julgadores neste mapa.")
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
