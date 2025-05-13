import pyterrier as pt
import pandas as pd
import argparse
from pyterrier.measures import MAP, nDCG, P
import matplotlib.pyplot as plt
import numpy as np

if not pt.started():
    pt.init()

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

vaswani = pt.get_dataset("vaswani")
index_path = "./index"

lambda_value = 0.6  # Segun terrier.org 'The default constructor uses the default value of lambda = 0.15'
mu_value = 2000  # Segun terrier.org 'This class sets mu to 2500 by default'

# Dejo la declaracion del parametro Mu, pero en base a mis pruebas, no parecen cambiar el comportamiento del retriever DirichletLM
# Por lo que creo que se esta utilizando el valor default 2500

TF_IDF = pt.terrier.Retriever(vaswani.get_index(), wmodel="TF_IDF")
DLM = pt.terrier.Retriever(
    vaswani.get_index(), wmodel="DirichletLM", controls={"DirichletLM.c": mu_value}
)
HIEMSTRA_LM = pt.terrier.Retriever(
    vaswani.get_index(),
    wmodel="Hiemstra_LM",
    controls={"hiemstra_lm.lambda": lambda_value},
)

topics = vaswani.get_topics()
qrels = vaswani.get_qrels()

qrels["qid"] = qrels["qid"].astype(str)
topics["qid"] = topics["qid"].astype(str)


# print(qrels.head())
# print(qrels.columns)


def globalMetrics() -> None:
    experiment = pt.Experiment(
        [TF_IDF, DLM, HIEMSTRA_LM], topics, qrels, [MAP, nDCG, P @ 10]
    )
    print(experiment)

    RECALL_POINTS = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

    for retriever in [TF_IDF, DLM, HIEMSTRA_LM]:
        run = retriever(topics)
        print(f"\nCurva R-P para {str(retriever)}:")

        all_interpolated_precisions = []

        for _, query in topics.iterrows():

            qid = query["qid"]
            relevant_docs = set(qrels[qrels["qid"] == qid]["docno"])
            retrieved_docs = run[run["qid"] == qid]["docno"].tolist()

            precisions = []
            recalls = []
            relevant_retrieved = 0

            # Aqui, acumulo la cantidad de documentos relevantes por cada posicion del ranking, y calculo su precision
            for i, doc in enumerate(retrieved_docs, start=1):
                if doc in relevant_docs:
                    relevant_retrieved += 1
                precision = relevant_retrieved / i
                recall = (
                    relevant_retrieved / len(relevant_docs)
                    if len(relevant_docs) > 0
                    else 0
                )
                precisions.append(precision)
                recalls.append(recall)

            # Ahora, calculo la interpolación en 11 puntos.
            interpolated_precisions = []
            for recall_level in RECALL_POINTS:
                # Obtengo las precisiones donde el recall es mayor o igual al nivel de recall actual (recall_level)
                valid_precisions = []
                for p, rc in zip(precisions, recalls):
                    if rc >= recall_level:
                        valid_precisions.append(p)

                # Si obtengo precisiones validas, hago append con la mayor.
                # Si no obtengo precisiones validas, hago append con la ultima precision interpolada cargada (o 0 de no haber)
                if valid_precisions:
                    max_precision = max(valid_precisions)
                else:
                    max_precision = (
                        interpolated_precisions[-1] if interpolated_precisions else 0
                    )
                interpolated_precisions.append(max_precision)
            all_interpolated_precisions.append(interpolated_precisions)

            """
            print(f"\nQID = {qid}")
            print(f"QUERY = {query_text}")
            print(f"Interpolated Precisions: {interpolated_precisions}")
            """

        # Calculo el promedio de precisiones interpoladas para cada nivel de recall de los 11 puntos estandar
        avg_precisions = np.mean(all_interpolated_precisions, axis=0)
        plt.plot(RECALL_POINTS, avg_precisions, marker="o", label=str(retriever))
        print(f"\nPromedio Interpolado de {str(retriever)}:\n{avg_precisions}")

    plt.title("Curvas de Precisión-Recall Interpoladas (11 puntos)")
    plt.xlabel("Recall estandar promedio")
    plt.ylabel("Precision interpolada promedio")
    plt.legend()
    plt.show()


def perqueryMetrics() -> None:
    experiment = pt.Experiment(
        [TF_IDF, DLM, HIEMSTRA_LM], topics, qrels, [MAP, nDCG, P @ 10], perquery=True
    )
    experiment["qid"] = experiment["qid"].astype(int)
    print(experiment.sort_values(by=["qid", "measure", "name"], ascending=True))

    metrics = experiment[["name", "measure", "value"]]

    models = metrics["name"].unique()

    # Por cada modelo de RI, extraigo las metricas que les corresponden, y hago un histograma por cada metrica.
    for model in models:
        modelSet = metrics[metrics["name"] == model]

        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle(f"Distribución de las métricas para {str(model)}", fontsize=16)

        measures = modelSet["measure"].unique()

        for i, measure in enumerate(measures):
            metric_values = modelSet[modelSet["measure"] == measure]["value"]
            axes[i].hist(metric_values, bins=15, color="skyblue", edgecolor="black")
            axes[i].set_title(f"{measure}")
            axes[i].set_xlabel(f"{measure} score")
            axes[i].set_ylabel("Frecuencia")
            axes[i].grid(axis="y", linestyle="--", alpha=0.7)

        plt.tight_layout()
        plt.subplots_adjust(top=0.85)
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Comparacion entre TF-IDF y modelos de lenguaje"
    )
    parser.add_argument(
        "perquery",
        nargs="?",
        type=str,
        help="Escribir <y> para hacer un analisis a nivel individual (por query), o deje en blanco para analizar a nivel global",
    )
    args = parser.parse_args()

    if not args.perquery:
        globalMetrics()
    elif args.perquery == "y":
        perqueryMetrics()
    else:
        print(
            "[ERROR]: Ingrese <y> para analizar a nivel individual, o deje en blanco para analizar a nivel global"
        )
