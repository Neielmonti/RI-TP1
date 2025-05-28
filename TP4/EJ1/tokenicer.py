from collections import defaultdict
import re
import heapq
import platform
import tempfile
import subprocess
import nltk
from nltk.corpus import stopwords


class TextProcessor:
    def __init__(self):
        nltk.download("stopwords", quiet=True)
        self.stopwords = set(stopwords.words("english"))

    def sort_words(self, text):
        if platform.system() == "Windows":
            return self._sort_words_windows(text)
        else:
            return self._sort_words_unix(text)

    def _sort_words_windows(self, text):
        heap = []
        for line in text.splitlines():
            line = re.sub(r"[^a-záéíóúñü\s]", " ", line.lower())
            for word in line.split():
                heapq.heappush(heap, word)
        words = []
        while heap:
            words.append(heapq.heappop(heap))
        return words

    def _sort_words_unix(self, text):
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(text)
            tmp.flush()
            command = (
                f"cat {tmp.name} | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\\n' "
                f"| sed 's/[^a-záéíóúñü]/ /g' | tr -s ' ' '\\n' | sed '/^$/d' | sort"
            )
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            return result.stdout.strip().split("\n")

    def process(self, text):
        words = self.sort_words(text)
        freqs = defaultdict(int)
        for word in words:
            if len(word) > 3 and word not in self.stopwords:
                freqs[word] += 1
        return freqs
