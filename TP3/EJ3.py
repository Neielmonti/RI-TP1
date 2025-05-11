import pyterrier as pt
from pyterrier.measures import MAP, nDCG, P
import pandas as pd
import pyterrier as pt

pd.set_option('display.max_rows', None)  # Muestra todas las filas
pd.set_option('display.max_columns', None)  # Muestra todas las columnas

pt.init()

vaswani = pt.get_dataset("vaswani")

index_path = "./index"

lambda_value = 0.5 # The default constructor uses the default value of lambda = 0.15.
mu_value = 1500 # This class sets mu to 2500 by default

TF_IDF = pt.terrier.Retriever(vaswani.get_index(), wmodel="TF_IDF")
DLM = pt.terrier.Retriever(vaswani.get_index(), wmodel="DirichletLM", controls={"DirichletLM.c": mu_value})
HIEMSTRA_LM = pt.terrier.Retriever(vaswani.get_index(), wmodel="Hiemstra_LM", controls={"hiemstra_lm.lambda": lambda_value})

topics = vaswani.get_topics()

experiment = pt.Experiment(
    [TF_IDF,DLM, HIEMSTRA_LM],
    topics,
    vaswani.get_qrels(),
    [MAP, nDCG, P@10]
    )

print(experiment)