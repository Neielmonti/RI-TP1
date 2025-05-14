import re
import platform
import heapq
import tempfile
import subprocess
from collections import defaultdict
import nltk
from nltk.corpus import stopwords
import struct
import pprint
import heapq
from collections import defaultdict
import pprint

class TextProcessor:
    def __init__(self):
        #self.token_count = 0
        #self.doc_count = 0

        nltk.download("stopwords")
        self.stopwords = set(stopwords.words("english"))
        #nltk.download("stopwords", quiet=True)
        #self.stopwords = set(stopwords.words("spanish"))

        self.json_data = defaultdict(
            lambda: {"postings": {}}
            )
        self.epoch = 0
        self.PATH_CHUNKS = "chunks/chunk"
        self.terms = []


    def process_text(self, text: str, docID: str):
        sorted_words = self.sort_words(text)

        i = 0
        while i < len(sorted_words):
            word = sorted_words[i]
            if word in self.stopwords or len(word) <= 3:
                i += 1
                continue
            term_count = 1
            while i + 1 < len(sorted_words) and sorted_words[i + 1] == word:
                term_count += 1
                i += 1
            if word not in self.stopwords:
                self.update_json_in_memory(word, docID, term_count)
            i += 1

        #self.doc_count += 1


    def sort_words(self, text):
        if platform.system() == "Windows":
            return self.sort_words_windows(text)
        else:
            return self.sort_words_unix(text)


    def sort_words_unix(self, text):
        with tempfile.NamedTemporaryFile(
            mode="w+",
            delete=False,
            encoding="utf-8"
        ) as tmp:
            tmp.write(text)
            tmp.flush()
            command = (
                f"cat {tmp.name} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\n' "
                f"| sed 's/[^a-záéíóúñü]/ /g' | tr -s ' ' '\n' | sed '/^$/d' | sort"
            )
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True)
            words = result.stdout.strip().split('\n')
            #self.token_count += len(words)
            return words


    def sort_words_windows(self, text):
        heap = []
        for line in text.splitlines():
            line = re.sub(r'[^a-záéíóúñü\s]', ' ', line.lower())
            for word in line.split():
                heapq.heappush(heap, word)
        words = []
        while heap:
            words.append(heapq.heappop(heap))
        #self.token_count += len(words)
        return words


    def update_json_in_memory(self, term, docID, freq):
        try:
            id = self.terms.index(term)
        except ValueError:
            id = len(self.terms)
            self.terms.append(term)

        term_data = self.json_data[id]
        term_data["postings"][docID] = freq


    def serializar(self):
        chunk_file = self.PATH_CHUNKS + str(self.epoch) + ".bin"

        print(f"analizando epoca {self.epoch}: ")
        pprint.pprint(self.json_data)

        with open(chunk_file, "wb") as p_file:
            # HACE FALTA ORDENARLOS ACA? TAL VEZ NO, Y TAL VEZ SOLO SUMA PROCESAMIENTO PARA NADA
            for term, data in sorted(self.json_data.items()):
                postings = data["postings"].items()
                for docID, freq in postings:
                    p_file.write(struct.pack("III", term, int(docID), freq))
        
        self.json_data.clear()
        self.json_data = defaultdict(
            lambda: {"postings": {}}
            )
        self.epoch += 1


    def getVocabulary(self):
        print(f"Epoch {self.epoch}")

        postings_lists = [[]] * len(self.terms)

        for i in range(self.epoch - 1):

            chunk_file = self.PATH_CHUNKS + str(i) + ".bin"
        
            with open(chunk_file, "rb") as p_file:
                while True:

                    data = p_file.read(12)
                    if not data:
                        break

                    posting = struct.unpack("III", data)
                    postings_lists[posting[0]].append((posting[1],posting[2]))
            
        return postings_lists


    def cargar_indice(self):
        voc = self.getVocabulary()
        print("LISTA DE POSTING_LISTS")
        for i in range(len(self.terms)):
            print(f"/nTermino: {self.terms[i]}/nPostings_list: {voc[i]}")
        #pprint.pprint(voc)