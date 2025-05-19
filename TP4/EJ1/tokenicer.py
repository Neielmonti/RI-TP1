import re
import platform
import tempfile
import subprocess
import nltk
from nltk.corpus import stopwords
import struct
import pickle
import heapq
from collections import defaultdict
import pprint
import os
from pathlib import Path

class TextProcessor:
    def __init__(self):
        nltk.download("stopwords")
        self.stopwords = set(stopwords.words("english"))

        self.json_data = defaultdict(
            lambda: {"postings": {}}
            )
    
        self.epoch = 0
        self.PATH_CHUNKS = Path("chunks") / "chunk"
        self.PATH_VOCAB = "vocabulary.bin"
        self.PATH_POSTINGS = "postings.bin"
        self.index = {}

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
            termid = self.terms.index(term)
        except ValueError:
            termid = len(self.terms)
            self.terms.append(term)

        term_data = self.json_data[termid]
        term_data["postings"][docID] = freq


    def serializar(self):
        chunk_file = self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{self.epoch}.bin"

        chunk_file.parent.mkdir(parents=True, exist_ok=True)

        print(f"analizando epoca {self.epoch}: ")

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

    '''
    def setVocabulary(self):
        print(f"Epoch {self.epoch}")

        postings_lists = [[] for _ in range(len(self.terms))]

        for i in range(self.epoch):
            chunk_file = self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{self.epoch}.bin"
        
            with open(chunk_file, "rb") as c_file:
                while True:

                    data = c_file.read(12)
                    if not data:
                        break

                    posting = struct.unpack("III", data)
                    postings_lists[posting[0]].append((posting[1],posting[2]))

        return postings_lists
    '''

    def setVocabulary(self):
        with open(self.PATH_VOCAB, "wb") as v_file, open(self.PATH_POSTINGS, "wb") as p_file:
            offset = 0
            vocab = {}
            
            for termID, term in enumerate(self.terms):
                postings_list = []

                for e in range(self.epoch):
                    chunk_file = self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{e}.bin"
                    if not chunk_file.exists():
                        print(f"[ERROR]: Archivo de chunk faltante: {chunk_file}")
                        return
                
                # Recorro todos los chunks para este término
                for epoch in range(self.epoch):
                    chunk_file = self.PATH_CHUNKS.parent / f"{self.PATH_CHUNKS.stem}{epoch}.bin"
                    with open(chunk_file, "rb") as c_file:
                        while True:
                            data = c_file.read(12)
                            if not data:
                                break
                            
                            tID, docID, freq = struct.unpack("III", data)
                            
                            if tID == termID:
                                postings_list.append((docID, freq))
                
                # Ordenar y consolidar postings
                postings_list.sort()
                df = len(postings_list)
                
                # Escribir postings a postings.bin
                for docID, freq in postings_list:
                    p_file.write(struct.pack("II", docID, freq))
                
                # Guardar en el diccionario de vocabulario
                vocab[term] = (offset, df)
                offset += len(postings_list) * 8  # Cada (docID, freq) ocupa 8 bytes
            
            # Serializar el vocabulario
            pickle.dump(vocab, v_file)

    def searchTerm(self, term: str) -> list:
        term_info = self.index.get(term)
        if not term_info:
            return []
        
        offset, df = term_info
        
        if not offset:
            return []
        
        postings = []
        with open(self.PATH_POSTINGS, "rb") as p_file:
            p_file.seek(offset)
            for _ in range(df):
                docID, freq = struct.unpack("II", p_file.read(8))
                postings.append((docID, freq))
        return postings

    
    def loadIndex(self):
        if not os.path.exists(self.PATH_VOCAB):
            print("[ERROR]: No existe un archivo de vocabulario")
            return
        with open(self.PATH_VOCAB, "rb") as v_file:
            vocab = pickle.load(v_file)
            self.index = {term: (offset, df) for term, (offset, df) in vocab.items()}
            pprint.pprint(self.index)
