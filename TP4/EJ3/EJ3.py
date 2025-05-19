from TP4.EJ2.taat import TaatRetriever
from TP4.EJ2.queryProcessor import QueryProcessor

"""
Utilizando el código e índice anteriores ejecute corridas  con el siguiente subset de queries 
(filtre solo los de 2 y 3 términos que estén en el vocabulario de su colección) y mida el tiempo de 
 ejecución en cada caso. Para ello, utilice los siguientes patrones booleanos:

    Queries |q| = 2
    t1 AND t2
    t1 OR t2
    t1 NOT t2

    Queries |q| = 3
    t1 AND t2 AND t3
    (t1 OR t2) NOT t3
    (t1 AND t2) OR t3
"""

queryProcessor = QueryProcessor()

def processQueries(path: str):
    with open(path, "r") as q_file:
        line = q_file.readline()
        while line:
            aux = line.split(":")
            terms = queryProcessor.process_query(aux[1])
            if len(terms) == 2 or len(terms) == 3:
                searchBooleanPattern(terms)
            line = q_file.readline()

def searchBooleanPattern(terms: list):
    pass
