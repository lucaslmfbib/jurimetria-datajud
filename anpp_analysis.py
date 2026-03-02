import os
import importlib
from typing import Any

import pandas as pd
import requests

try:
    from IPython.display import display
except Exception:
    def display(obj: Any) -> None:
        print(obj)


URL = "https://api-publica.datajud.cnj.jus.br/api_publica_tjmg/_search"
API_KEY = os.getenv("DATAJUD_API_KEY", "")


def to_sao_paulo_datetime(value: Any) -> pd.Timestamp | pd.NaT:
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


def fetch_data() -> list[dict[str, Any]]:
    if not API_KEY:
        raise RuntimeError(
            "Defina a variável de ambiente DATAJUD_API_KEY com seu token."
        )

    payload = {
        "size": 10000,
        "query": {"match": {"classe.codigo": 12729}},
        "sort": [{"dataAjuizamento": {"order": "desc"}}],
    }
    headers = {
        "Authorization": API_KEY,
        "Content-Type": "application/json",
    }

    response = requests.post(URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data.get("hits", {}).get("hits", [])


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
    df["assuntos"] = df["assuntos"].apply(parse_assuntos)
    df["movimentos"] = df["movimentos"].apply(parse_movimentos)
    df["movimentos"] = df["movimentos"].apply(
        lambda x: sorted(x, key=lambda tup: tup[2] if len(tup) > 2 else pd.NaT, reverse=True)
    )
    df["data_ajuizamento"] = df["data_ajuizamento"].apply(to_sao_paulo_datetime)
    df["ultima_atualizacao"] = df["ultima_atualizacao"].apply(to_sao_paulo_datetime)
    return df


def charts(df_anpp: pd.DataFrame) -> None:
    try:
        plt = importlib.import_module("matplotlib.pyplot")
    except Exception:
        print("matplotlib não está instalado; gráficos foram ignorados.")
        return

    contagem = (
        df_anpp["data_ajuizamento"].dt.hour.value_counts().sort_index()
    )

    plt.figure(figsize=(12, 6))
    contagem.plot(kind="bar", color="skyblue")
    plt.title("Horário de ajuizamento dos ANPPs")
    plt.xlabel("Hora")
    plt.ylabel("Número de ajuizamentos")
    plt.grid(axis="y", alpha=0.8)
    plt.savefig("horario_anpp.jpg")
    plt.show()

    ajuizamentos_expediente = contagem[8:19].sum()
    ajuizamentos_fora = contagem[0:8].sum() + contagem[19:].sum()

    labels = ["Das 9h às 19h", "Fora do expediente"]
    sizes = [ajuizamentos_expediente, ajuizamentos_fora]
    colors = ["lightblue", "lightgreen"]
    explode = (0.1, 0)

    plt.figure(figsize=(8, 6))
    plt.pie(
        sizes,
        explode=explode,
        labels=labels,
        colors=colors,
        autopct="%1.2f%%",
        startangle=45,
    )
    plt.title("Ajuizamento de ANPPs")
    plt.axis("equal")
    plt.savefig("pizza_anpp.jpg")
    plt.show()

    df_resampled = df_anpp.set_index("data_ajuizamento").resample("ME").size()

    x = [f"Mês de {str(index)[:7]}" for index, _ in df_resampled.items()]
    y = [value for _, value in df_resampled.items()]

    plt.figure(figsize=(12, 6))
    plt.bar(x, y, color="orange")
    plt.xlabel("Meses")
    plt.ylabel("Ajuizamentos")
    plt.title("ANPPs ajuizados")
    plt.xticks(rotation=45, ha="right")

    for i in range(len(x)):
        plt.text(x=x[i], y=y[i] + 10, s=str(y[i]), ha="center")

    plt.tight_layout()
    plt.savefig("n_ajuizamentos_anpp.jpg")
    plt.show()


def export_top_100(df_anpp: pd.DataFrame) -> None:
    top_100 = (
        df_anpp.groupby(["municipio", "orgao_julgador"])["codigo"]
        .count()
        .sort_values(ascending=False)
        .head(100)
    )

    with open("top_100_ajuizamentos_anpp.txt", "w") as file:
        for index, value in top_100.items():
            texto = f"{index[0]} | {index[1]} | {value}"
            file.write(texto + "\n")
            print(texto)


def main() -> None:
    hits = fetch_data()
    print(f"Total de registros retornados: {len(hits)}")
    display(hits[:10])

    df_anpp = hits_to_dataframe(hits)
    print(df_anpp.head())

    df_anpp.to_csv("anpp.csv", sep=",", header=True, index=False)
    with open("movimentos_anpp.txt", "w") as file:
        file.write("Exemplo de conteúdo no arquivo de texto.")

    df_anpp.info()
    if "assuntos" in df_anpp.columns:
        print(df_anpp["assuntos"].value_counts())

    charts(df_anpp)
    export_top_100(df_anpp)


if __name__ == "__main__":
    main()
