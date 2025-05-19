import re
import platform
import tempfile
import subprocess
import nltk
from nltk.corpus import stopwords
import heapq

class QueryProcessor:
    def __init__(self):
        nltk.download("stopwords")
        self.stopwords = set(stopwords.words("english"))

    def process_query(self, text: str):
        output = []
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
                output.append((word, term_count))
            i += 1
        return output


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
        return words
    
def main():
    print("waeasdasdasd")
    pass

if __name__ == "__main__":
    main()