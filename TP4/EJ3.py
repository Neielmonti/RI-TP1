import pyterrier as pt
from pyterrier.measures import MAP, nDCG, P
import pandas as pd

pd.set_option('display.max_rows', None)  # Muestra todas las filas
pd.set_option('display.max_columns', None)  # Muestra todas las columnas

pt.init()

vaswani = pt.get_dataset("vaswani")

print("Files in vaswani corpus: %s " % vaswani.get_corpus())

index_path = "./index"

"""

indexer = pt.TRECCollectionIndexer(index_path, blocks=True)

indexref = indexer.index(vaswani.get_corpus())

indexref.toString()

index = pt.IndexFactory.of(indexref)
print(index.getCollectionStatistics().toString())

"""

TF_IDF = pt.terrier.Retriever(vaswani.get_index(), wmodel="TF_IDF")
DLM = pt.terrier.Retriever(vaswani.get_index(), wmodel="DirichletLM")

topics = vaswani.get_topics().head(11)

experiment = pt.Experiment(
    [TF_IDF,DLM],
    topics,
    vaswani.get_qrels(),
    [MAP, nDCG, P@10],
    perquery=True)

experiment = experiment.sort_values(by=['qid','name','measure'])

print(experiment)