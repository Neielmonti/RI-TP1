file = open("palabrasRI.txt","r")

def searchWord(searchedWord,l):
    for i in range(len(l)):
        if l[i][0] == searchedWord:
            return i
    return -1

def getFrequencies(l):
    salida = []
    for i in range(len(l)):
        index = searchWord(l[i], salida)
        if (index != -1):
            salida[index][1] = salida[index][1] + 1
        else:
            salida.append([l[i],1])
    return salida

def getFreqTuple(x):
    return x[1]

ms = file.read()
splited = ms.split("\n")

freq = getFrequencies(splited)
output = sorted(freq, key=lambda x: x[1])

def getUniqueWords(list):
    salida = []
    unique = True
    index = 0
    while unique and (index < len(list)):
        if list[index][1] == 1:
            salida.append(list[index][0])
        else:
            unique = False
        index = index + 1
    return salida

print(output)
print("ETC \n")
uniqueWords = getUniqueWords(output)

print("Palabra menos frecuente: " , output[0][0] , "\n")
print("Palabra mas frecuente: " + output[len(output)-1][0] + "\n")
print("Palabras unicas: " , uniqueWords , "\n")
print("Palabras con su frecuencia: " , output , "\n")
