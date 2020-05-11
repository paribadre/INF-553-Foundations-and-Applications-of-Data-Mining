from pyspark import SparkContext
from collections import Counter
from itertools import combinations
import itertools
import csv
import sys
import time
import os
import math


def countOfSingletons(basket):
    countList = {}

    for j in basket:
        for i in j:
            if i in countList.keys():
                countList[i] += 1
            else:
                countList[i] = 1

    return countList


def countOfMultiple(basket, cand_list):
    countList = {}

    for tup in cand_list:
        for singlebasket in basket:
            if set(tup).issubset(singlebasket):
                if tup in countList:
                    countList[tup] += 1
                else:
                    countList[tup] = 1
    return countList


def pruneCountDict(countDict, prob_support):
    candList = []
    for key, value in countDict.items():
        if value >= prob_support:
            candList.append(key)
    return candList


def calculateCurrentFrequent(basket, candidate_list, prob_support):
    countK = {}
    for candidate in candidate_list:
        candidate = set(candidate)
        key = tuple(sorted(candidate))

        for b in basket:
            if candidate.issubset(b):
                if key in countK:
                    countK[key] = countK[key] + 1
                else:
                    countK[key] = 1

    currentFrequent = sorted(pruneCountDict(countK, prob_support))

    return currentFrequent


def apriori(basket, Count, lsupport):
    basket = list(basket)
    prob_support = lsupport * (float(len(basket)) / float(Count))
    result = list()

    countSingles = countOfSingletons(basket)
    finalSingletons = sorted(pruneCountDict(countSingles, prob_support))
    # for i in finalSingletons:
    #     result.append(tuple({i}))

    k = 2
    generatedPairs = list(itertools.combinations(finalSingletons, 2))
    countDoubles = countOfMultiple(basket, generatedPairs)
    finalDoubles = sorted(pruneCountDict(countDoubles, prob_support))
    prevFrequentFinal = finalDoubles

    k += 1
    while len(prevFrequentFinal) != 0:
        KCandidateItems = []
        for i in range(len(prevFrequentFinal) - 1):
            for j in range(i + 1, len(prevFrequentFinal)):
                a = prevFrequentFinal[i]
                b = prevFrequentFinal[j]
                if a[0:(k - 2)] == b[0:(k - 2)]:
                    KCandidateItems.append(list(set(a) | set(b)))
                else:
                    break
        currentFrequentFinal = calculateCurrentFrequent(basket, KCandidateItems, prob_support)
        prevFrequentFinal = sorted(list(set(currentFrequentFinal)))
        result.extend(currentFrequentFinal)
        k += 1

    result.extend(finalSingletons)
    result.extend(finalDoubles)
    return result


def generate_truly_freq_itemset(chunk, candidate_list):
    truly_frequent_itemlist = []
    truly_frequent_itemset = {}

    for basket in list(chunk):
        for candidate in candidate_list:
            # If the basket and candidate is a single integer
            if len(basket) == 1:
                if not isinstance(candidate, tuple):
                    if candidate == basket:
                        if candidate in truly_frequent_itemset:
                            truly_frequent_itemset[candidate] += 1
                        else:

                            truly_frequent_itemset[candidate] = 1
            else:
                # If candidate is single integer
                if not isinstance(candidate, tuple):
                    if candidate in basket:
                        if candidate in truly_frequent_itemset:
                            truly_frequent_itemset[candidate] += 1
                        else:
                            truly_frequent_itemset[candidate] = 1
                # If candidate is a double, triple, etc.
                else:
                    if set(candidate).issubset(basket):
                        if candidate in truly_frequent_itemset:
                            truly_frequent_itemset[candidate] += 1
                        else:
                            truly_frequent_itemset[candidate] = 1

    for key, v in truly_frequent_itemset.items():
        truly_frequent_itemlist.append((key, v))

    return truly_frequent_itemlist


if __name__ == '__main__':
    start = time.time()

    threshold = int(sys.argv[1])
    support = int(sys.argv[2])
    raw_data = sys.argv[3]
    outputFile = sys.argv[4]

    sc = SparkContext("local[*]", "SON")
    rdd = sc.textFile(raw_data)
    header = rdd.first()
    baskets = rdd.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x: (x[0].strip('"'), x[1].strip('"'),
                                                                                          int(x[5].strip('"'))))
    with open("task2.csv", "w") as fout:
       fout.write("DATE-CUSTOMER_ID, PRODUCT_ID\n")
       for x in baskets.collect():
           fout.write(x[0]+"-"+x[1]+","+str(x[2])+"\n")
    sc.stop()

    sc = SparkContext("local[*]", "SON")
    input = sc.textFile("task2.csv", minPartitions=None)
    inputchunk = input.mapPartitions(lambda x: csv.reader(x))

    header = inputchunk.first()
    inputchunk = inputchunk.filter(lambda x: x != header)
    totalBaskets = inputchunk.map(lambda x: (x[0], int(x[1]))).groupByKey().mapValues(set)\
        .filter(lambda x: len(x[1]) > threshold).map(lambda x: x[1])

    Count = totalBaskets.count()

    mapPhase1 = totalBaskets.mapPartitions(lambda b: apriori(b, Count, support)).map(lambda x: (x, 1))
    reducePhase1 = mapPhase1.reduceByKey(lambda x, y: 1).keys().collect()

    mapPhase2 = totalBaskets.mapPartitions(lambda x: generate_truly_freq_itemset(x, reducePhase1))
    reducePhase2 = mapPhase2.reduceByKey(lambda x, y: (x + y))

    Result = reducePhase2.filter(lambda x: x[1] >= support).map(lambda x: x[0])

    Dict = {}
    maxkey = 0
    for element in reducePhase1:
            if isinstance(element, int):
                key=1
                if key in Dict:
                    Dict[key].append(str(element))
                else:
                    if key > maxkey:
                        maxkey = key
                    Dict[key] = []
                    Dict[key].append(str(element))
            else:
                temp = tuple()
                for i in element:
                    temp = temp + (str(i),)
                key = len(temp)
                if key > maxkey:
                    maxkey = key
                if key in Dict:
                    Dict[key].append(sorted(temp))
                else:
                    Dict[key] = []
                    Dict[key].append(sorted(temp))

    candidate_output = ""
    for key in range(1, maxkey+1):
        Dict[key] = sorted(Dict[key])
        if key == 1:
            for element in Dict[key]:
                candidate_output = candidate_output + "('" + str(element) + "'),"
        else:
            for element in Dict[key]:
                candidate_output = candidate_output + str(tuple(element)) + ","
        candidate_output = candidate_output[:-1] + "\n\n"

    out = Result.collect()
    freqDict = {}
    freqmaxkey = 0
    for element in out:
            if isinstance(element, int):
                key = 1
                if key in freqDict:
                    freqDict[key].append(str(element))
                else:
                    if key > freqmaxkey: freqmaxkey = key
                    freqDict[key] = []
                    freqDict[key].append(str(element))
            else:
                temp = tuple()
                for i in element:
                    temp = temp + (str(i),)
                key = len(temp)
                if key > freqmaxkey:
                    freqmaxkey = key
                if key in freqDict:
                    freqDict[key].append(sorted(temp))
                else:
                    freqDict[key] = []
                    freqDict[key].append(sorted(temp))

    frequent_output = ""
    for key in range(1, freqmaxkey+1):
        freqDict[key] = sorted(freqDict[key])
        if key == 1:
            for element in freqDict[key]:
                frequent_output = frequent_output + "('" + str(element) + "'),"
        else:
            for element in freqDict[key]:
                frequent_output = frequent_output + str(tuple(element)) + ","
        frequent_output = frequent_output[:-1] + "\n\n"

    with open(outputFile, 'w') as fout:
        fout.write("Candidates:\n")
        fout.write(candidate_output[:-1])
        fout.write("\nFrequent Itemsets:\n")
        fout.write(frequent_output[:-1]+"\n")
    end = time.time()
    print("Duration: ", end - start)
