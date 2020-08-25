# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'

from typing import List
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import sys
import glob
import re
import copy

import settings
from settings import QUERY_ARGS


class Analyzer:

    CSV_SPECIFIER = ""

    # ATTENTION: This structure must be the same with generate_queries
    queryArg = copy.deepcopy(settings.QUERY_ARGS)

    # Column names that all queries have in common
    mainHeader = settings.MAIN_HEADER

    # Specific column names per Query
    headers = settings.HEADER_PER_QUERY
    # how many columns are related for rates.
    ratesPerQuery = settings.RATES_PER_QUERY

    def __init__(self, specifier, queryName="all", action=None):
        valid_specifiers = settings.VALID_CSV_SPECIFIERS
        PATH = settings.PATH
        if specifier:
            self.fileName = PATH + queryName + "_" + self.CSV_SPECIFIER + ".csv"
        else:
            self.fileName = PATH + queryName + ".csv"
        if specifier not in valid_specifiers:
            print(specifier + " is not a valid specifier @ analyzer.py")
            exit(-1)

        if queryName == "all": 
            # not using settings.PATH because vscode has different relative paths.
            for file in glob.glob(settings.PATH + "/*_"+ specifier + ".csv"):
                self.fileName = file
                self.queryName = os.path.basename(file).split("_")[0]  # remove extension
                print(self.fileName)
                print(self.queryName)
                print(action)
                self.setQuery()
                if action == "scatter":
                    if "equal" in specifier:
                        self.analyzeDistributionParallelism()
                    elif "metric" in specifier:
                        self.analyzeDistribution(False)
        elif queryName in self.headers:
            print("Reading file: " + self.fileName)
            self.queryName = queryName
            self.setQuery()
            if action == 'extract':
                pass
                #self.calcEfficientMetrics()
            elif action == "scatter":
                if "equal" in specifier:
                    #self.analyzeParallelization(True)
                    # self.analyzeDistributionEqual()
                    self.analyzeDistributionParallelism()
                elif "metric" in specifier:
                    #self.analyzeParallelization(False)
                    self.analyzeDistribution(False)
                elif specifier == "":
                    self.analyzeDistributionEqual()
            elif action == "test":
                df = self.aggregate_func(
                    self.data[(self.data["total_op_instances"] == 2.0)
                              & (self.data["operator"] == "Mapper")], 2)
                print(df.tail(15))
            else:
                print("Inserted invalid action [" + action +
                      "] in analyzer.py")

    def setQuery(self):
        """ Read the corresponding csv file for the set queryName
            Store the results in a pandas.Dataframe and filter out duplicate/NaN rows """
        print(self.fileName)
        self.data = pd.read_csv(self.fileName,
                                names=self.mainHeader[:2] +
                                self.headers[self.queryName] +
                                self.mainHeader[2:],delimiter=',', index_col=False)
        print(self.mainHeader[:2] +
                                self.headers[self.queryName] +
                                self.mainHeader[2:])
        print("Starting df len:" + str(len(self.data)))
        print(self.data.head())
        # remove any row that has a NaN value. Removing the rows with no results.
        # Bug on the my_db.py or monitor.py.
        self.data = self.data.dropna()
        print("REMOVED NA. Current df len: " + str(len(self.data)))

    
    def aggregate_func(self, curDF, parallelismVal):
        uniqueParallel = curDF["total_op_instances"].unique()
        print(uniqueParallel)
        """
        Aggregate all the parallel instances of an operator.
        DF: must be related to only 1 operator with a specific parallelism value.
        """
        if len(uniqueParallel) != 1 and uniqueParallel[0] != [parallelismVal]:
            print(
                "Aggragate function error. Dataframe contains more parallel instances than the given "
                + str(parallelismVal))
            raise Exception("Wrong input in aggregate_func")
        columns = list(curDF.columns)
        aggregation_functions = {c: "first" for c in columns[:-4]}
        for c in columns[-4:]:
            aggregation_functions[c] = "sum"
        aggregation_functions["op_instance_id"] = "sum"
        aggregation_functions["total_op_instances"] = "sum"

        sortingList = ["id", "epoch_timestamp"]
        sortingList.extend(settings.RATES_NAMES_PER_QUERY[self.queryName])
        #sortingList.append("op_instance_id")

        sortDF = curDF.sort_values(by=sortingList)
        sortDF = sortDF.reset_index(drop=True)
        # aggregate. We observe that op_instance_id is 6 (1+2+3) and total_op_instances is 9 (3+3+3) as we wanted
        return sortDF.groupby(
            sortDF.index //
            parallelismVal).aggregate(aggregation_functions).reindex(
                columns=curDF.columns)
    """
    def calcEfficientMetrics(self, percent=0.05):
        \"""
        Looks the {percent} of the maximun output rates and saves the mean input values of paralelization and rates.
        # WRONG WAY of calculate efficient metrics since we care about all the spectrum of the data not the 5% of the best.
        \"""
        startIndex = len(self.queryArg[self.queryName]) + 4
        uniqueOperators = self.data["operator"].unique()
        file_path = settings.PATH + "plots/" + self.queryName + "/" + "bestResults_" + self.CSV_SPECIFIER + ".txt"
        skipSet = set({0, 'NaN', 'nan'})
        with open(file_path, 'w+') as f:
            for operator in sorted(uniqueOperators):
                df = self.data[self.data["operator"] == operator]
                f.write("OPERATOR__ID : " + operator + "\n")
                print("OPERATOR__ID : " + operator)
                for i in range(startIndex, startIndex + 4):
                    currentOutputRateName = df.columns[i]
                    bestDF = df.sort_values(by=currentOutputRateName,
                                            ascending=False)
                    uniqueValues = int(
                        sum(bestDF[currentOutputRateName].unique()))
                    if uniqueValues in skipSet:
                        continue
                    print("------------" + currentOutputRateName +
                          "------------")
                    print(uniqueValues)
                    print(len(bestDF))
                    bestDF = bestDF.head(round(percent * len(bestDF)))
                    print(len(bestDF))
                    curOutputRateMeanVal = round(
                        bestDF[currentOutputRateName].mean(), 2)
                    f.write("--------" + currentOutputRateName + " == " +
                            str(curOutputRateMeanVal) + "--------" + "\n")
                    #top20Percent
                    print(currentOutputRateName)
                    for key in self.headers[self.queryName]:
                        mean = round(bestDF[key].mean(), 2)
                        result = f'\t\t{key} == {mean} \n'
                        f.write(result)
                    f.write("\n")
                    print()

    def analyzeRates(self):
        data = self.data
        uniqueOperators = data["operator"].unique()
        print(uniqueOperators)
        for operator in uniqueOperators:
            data = self.data[self.data["operator"] == operator]
            for rate in range(self.ratesPerQuery[self.queryName]):
                uniqueRates = data.iloc[:, rate + 1].unique()
                uniqueRates = np.sort(uniqueRates)
                meanTrueProc = []
                meanTrueOut = []
                meanObservedOut = []
                meanObservedProc = []
                for i, rateValue in enumerate(uniqueRates):
                    meanDF = data[data.iloc[:, rate + 1] == rateValue]
                    if self.ratesPerQuery[self.queryName] > 1:
                        if (len(meanDF.iloc[:, rate if rate ==
                                            uniqueRates[-1] else rate +
                                            2].unique()) > 1):
                            uniqueRates = np.delete(uniqueRates, i)
                            continue
                    meanDFtrueProc = meanDF["true_processing_rate"].mean()
                    meanDFtrueOut = meanDF["true_output_rate"].mean()
                    meanDFobservedOut = meanDF["observed_output_rate"].mean()
                    meanDFobservedProc = meanDF[
                        "observed_processing_rate"].mean()
                    meanTrueOut.append(meanDFtrueOut)
                    meanTrueProc.append(meanDFtrueProc)
                    meanObservedOut.append(meanDFobservedOut)
                    meanObservedProc.append(meanDFobservedProc)
                self.plot_graph(
                    f"{operator}__{data.columns[rate+1]}_TrueOutput",
                    data.columns[rate + 1],
                    "true_output_rate",
                    uniqueRates,
                    meanTrueOut,
                )
                self.plot_graph(
                    f"{operator}__{data.columns[rate+1]}_TrueProc",
                    data.columns[rate + 1],
                    "true_processing_rate",
                    uniqueRates,
                    meanTrueProc,
                )
                self.plot_graph(
                    f"{operator}__{data.columns[rate+1]}_ObservedOutput",
                    data.columns[rate + 1],
                    "observed_output_rate",
                    uniqueRates,
                    meanObservedOut,
                )
                self.plot_graph(
                    f"{operator}__{data.columns[rate+1]}_ObservedProc",
                    data.columns[rate + 1],
                    "observed_processing_rate",
                    uniqueRates,
                    meanObservedProc,
                )

    def analyzeParallelization(self, equalParallelizationOperators=False):
        data = self.data
        uniqueOperators = data["operator"].unique()
        numberOfOperators = (len(data.columns) - len(self.mainHeader) -
                             self.ratesPerQuery[self.queryName])

        for operator in uniqueOperators:
            operatorDF = data[data["operator"] == operator]
            print("SELECTED operator:" + operator)
            parallelOperators = [i for i in range(1, numberOfOperators + 1)]
            # queries generated with the same parallelization values for all operators
            uniqueParallelizationArray = operatorDF.iloc[:, self.ratesPerQuery[
                self.queryName] + parallelOperators[0]].unique()
            print(uniqueParallelizationArray)

            for parallelColumn in parallelOperators:
                meanTrueProc = [[]
                                for i in range(len(uniqueParallelizationArray))
                                ]
                meanTrueOut = [[]
                               for i in range(len(uniqueParallelizationArray))]
                meanObservedOut = [
                    [] for i in range(len(uniqueParallelizationArray))
                ]
                meanObservedProc = [
                    [] for i in range(len(uniqueParallelizationArray))
                ]
                uniqueRatesList = [
                    [] for i in range(len(uniqueParallelizationArray))
                ]
                for parallelIndex, parallelizationValue in enumerate(
                        uniqueParallelizationArray):
                    parallelOperatorName = operatorDF.columns[
                        self.ratesPerQuery[self.queryName] + parallelColumn]
                    df = None
                    if equalParallelizationOperators:
                        for parallelColumn in parallelOperators:
                            parallelOperatorName = operatorDF.columns[
                                self.ratesPerQuery[self.queryName] +
                                parallelColumn]
                            print("OPE_PARAL" + str(parallelColumn) + " :" +
                                  parallelOperatorName + "==" +
                                  str(parallelizationValue))
                            df = operatorDF[
                                operatorDF.
                                iloc[:, self.ratesPerQuery[self.queryName] +
                                     parallelColumn] == parallelizationValue]
                    else:
                        # Incrementing only one operator at a time and keep the rest equal to 1
                        print("OPE_PARAL:" + parallelOperatorName + "==" +
                              str(parallelizationValue))
                        df = operatorDF[
                            operatorDF.
                            iloc[:, self.ratesPerQuery[self.queryName] +
                                 parallelColumn] == parallelizationValue]
                    rateName = ""
                    if not df:
                        continue
                    for rate in range(1,
                                      self.ratesPerQuery[self.queryName] + 1):
                        rateName = df.columns[rate]
                        print("SELECTED RATE:" + rateName)
                        uniqueRates = df.iloc[:, rate].unique()
                        uniqueRates = np.sort(uniqueRates)
                        if len(uniqueRates) == 0:
                            print(f"{operator}::  {rateName} NO VALUES?")
                        uniqueRatesList[parallelIndex].extend(
                            uniqueRates.tolist())
                        for i, rateValue in enumerate(uniqueRates):
                            meanDF = df[df.iloc[:, rate] == rateValue]
                            secondRate = meanDF.iloc[:, rate +
                                                     1 if rate == 1 else rate -
                                                     1].unique()
                            if self.ratesPerQuery[self.queryName] > 1:
                                equalToSecondRate = self.queryArg[
                                    self.queryName][list(
                                        self.queryArg[self.queryName])
                                                    [rate if rate == 1 else 0]]
                                meanDF = meanDF[
                                    meanDF.iloc[:, rate +
                                                1 if rate == 1 else rate -
                                                1] == equalToSecondRate]
                                cn = meanDF.columns[rate +
                                                    1 if rate == 1 else rate -
                                                    1]
                                uniqueSRate = meanDF.iloc[:,
                                                          rate + 1 if rate ==
                                                          1 else rate -
                                                          1].unique()
                            print(
                                f"{operator}:: {parallelOperatorName}=={parallelizationValue} && {rateName} == {rateValue} {equalToSecondRate} == {cn} || {uniqueSRate}"
                            )

                            # print(meanDF.iloc[:, rate +1 if rate == 1 else rate -1].unique()[:10])
                            # print(meanDF.head(20))
                            # print(meanDF)
                            # print()
                            # if self.ratesPerQuery[self.queryName] > 1:
                            # if len(meanDF.iloc[:, rate if rate == uniqueRates[-1] else rate + 2].unique()) > 1:
                            #     uniqueRates = np.delete(uniqueRates, i)
                            # continue
                            meanDFtrueProc = meanDF[
                                "true_processing_rate"].mean()
                            meanDFtrueOut = meanDF["true_output_rate"].mean()
                            meanDFobservedOut = meanDF[
                                "observed_output_rate"].mean()
                            meanDFobservedProc = meanDF[
                                "observed_processing_rate"].mean()
                            meanTrueOut[parallelIndex].append(meanDFtrueOut)
                            meanTrueProc[parallelIndex].append(meanDFtrueProc)
                            meanObservedOut[parallelIndex].append(
                                meanDFobservedOut)
                            meanObservedProc[parallelIndex].append(
                                meanDFobservedProc)
                    if equalParallelizationOperators:
                        parallelOperatorName = ""
                    else:
                        parallelOperatorName = parallelOperatorName[:6]
                    rateName = rateName[:8]
                    operator = operator[:8]
                    self.multiple_lines_plot(
                        f"{operator}__{rateName}_{parallelOperatorName}_TrueOutput",
                        rateName,
                        "MeanTrueOutput",
                        uniqueRatesList,
                        meanTrueOut,
                    )
                    self.multiple_lines_plot(
                        f"{operator}__{rateName}_{parallelOperatorName}_TrueProc",
                        rateName,
                        "MeanTrueProcessing",
                        uniqueRatesList,
                        meanTrueProc,
                    )
                    self.multiple_lines_plot(
                        f"{operator}__{rateName}_{parallelOperatorName}_ObservedOutput",
                        rateName,
                        "MeanObservedOutput",
                        uniqueRatesList,
                        meanObservedOut,
                    )
                    self.multiple_lines_plot(
                        f"{operator}__{rateName}_{parallelOperatorName}_ObservedProc",
                        rateName,
                        "MeanObservedProcessing",
                        uniqueRatesList,
                        meanObservedProc,
                    )
                # All parallelizationOperators are equal so do not loop through again
                if equalParallelizationOperators:
                    break

    def analyzeDistributionEqual(self):
        print("EQUAL DISTRIBUTION")
        data = self.data
        uniqueOperators = data["operator"].unique()
        numberOfOperators = (len(data.columns) - len(self.mainHeader) -
                             self.ratesPerQuery[self.queryName])

        for operator in uniqueOperators:
            print("SELECTED operator:" + operator)
            operatorDF = data[data["operator"] == operator]

            parallelOperators = [i for i in range(1, numberOfOperators + 1)]
            for parallelColumn in parallelOperators:
                uniqueParallelizationArray = \
                    operatorDF.iloc[:, self.ratesPerQuery[self.queryName] +parallelOperators[0]].unique()
                uniqueTrueProc = [
                    [] for i in range(len(uniqueParallelizationArray))
                ]
                uniqueTrueOut = [
                    [] for i in range(len(uniqueParallelizationArray))
                ]
                uniqueObservedOut = [
                    [] for i in range(len(uniqueParallelizationArray))
                ]
                uniqueObservedProc = [
                    [] for i in range(len(uniqueParallelizationArray))
                ]
                for parallelIndex, parallelizationValue in enumerate(
                        uniqueParallelizationArray):
                    parallelOperatorName = operatorDF.columns[
                        self.ratesPerQuery[self.queryName] + parallelColumn]
                    df = None
                    # assign the selected parallelism value to all operators
                    for parallelColumn in parallelOperators:
                        parallelOperatorName = operatorDF.columns[
                            self.ratesPerQuery[self.queryName] +
                            parallelColumn]
                        print("PARALLELISM:: " + str(parallelColumn) + " :" +
                              parallelOperatorName + "==" +
                              str(parallelizationValue))
                        df = operatorDF[
                            operatorDF.
                            iloc[:, self.ratesPerQuery[self.queryName] +
                                 parallelColumn] == parallelizationValue]

                    rateName = ""
                    print(len(df))
                    for primaryRate in range(
                            1, self.ratesPerQuery[self.queryName] + 1):
                        rateName = df.columns[primaryRate]
                        print("SELECTED RATE:" + rateName)

                        uniqueRates = df.iloc[:, primaryRate].unique()
                        uniqueRates = np.sort(uniqueRates)

                        # NOTE:
                        if len(uniqueRates) == 0:
                            print(f"{operator}::  {rateName} NO VALUES?")

                        # uniqueRates = [
                        #     uniqueRates[index]
                        #     for index in range(len(uniqueRates))
                        #     if index % 2 != 0
                        # ]

                        for i, rateValue in enumerate(uniqueRates):
                            meanDF = df[df.iloc[:, primaryRate] == rateValue]
                            # secondRate = meanDF.iloc[:, primaryRate +
                            #                          1 if primaryRate == 1 else primaryRate -
                            #                          1].unique()

                            # Reduce noise - Select the default values for the rest rates
                            for secondaryRate in range(
                                    1, self.ratesPerQuery[self.queryName] + 1):
                                if secondaryRate == primaryRate:
                                    continue
                                keys = list(
                                    self.queryArg[self.queryName].keys())
                                print("Secondrate selected :" +
                                      keys[secondaryRate - 1] + "==" +
                                      str(self.queryArg[self.queryName][keys[
                                          secondaryRate - 1]]) + " len of df" +
                                      str(len(meanDF)))
                                meanDF = meanDF[meanDF.iloc[:,
                                                            secondaryRate] ==
                                                self.queryArg[self.queryName][
                                                    keys[secondaryRate - 1]]]
                                print(len(meanDF))

                            parallelOperatorName = "Parallelism"
                            rateName = rateName[:8]
                            operator = operator[:8]
                            uniqueDFtrueProc = meanDF[
                                "true_processing_rate"].unique()
                            uniqueDFtrueOut = meanDF[
                                "true_output_rate"].unique()
                            uniqueDFobservedOut = meanDF[
                                "observed_output_rate"].unique()
                            uniqueDFobservedProc = meanDF[
                                "observed_processing_rate"].unique()
                            #print(uniqueDFobservedProc)
                            uniqueTrueOut[parallelIndex].extend(
                                uniqueDFtrueOut)
                            uniqueTrueProc[parallelIndex].extend(
                                uniqueDFtrueProc)
                            uniqueObservedOut[parallelIndex].extend(
                                uniqueDFobservedOut)
                            uniqueObservedProc[parallelIndex].extend(
                                uniqueDFobservedProc)

                    legend = [
                        "v=" + str(i) for i in uniqueParallelizationArray
                    ]

                    self.plot_scatter_ecdf(
                        uniqueTrueOut,
                        "TrueOutput",
                        f"{operator}__{rateName}_{parallelOperatorName}={parallelizationValue}_MeanTrueOutput",
                        legend=legend)
                    self.plot_scatter_ecdf(
                        uniqueTrueProc,
                        "TrueProcessing",
                        f"{operator}__{rateName}_{parallelOperatorName}={parallelizationValue}_TrueProcessing",
                        legend=legend)
                    self.plot_scatter_ecdf(
                        uniqueObservedOut,
                        "ObservedOutput",
                        f"{operator}__{rateName}_{parallelOperatorName}={parallelizationValue}_ObservedOutput",
                        legend=legend)
                    self.plot_scatter_ecdf(
                        uniqueObservedProc,
                        "ObservedProcessing",
                        f"{operator}__{rateName}_{parallelOperatorName}={parallelizationValue}_ObservedProcessing",
                        legend=legend)
                break
    """

    def analyzeDistribution(self, equalParallelizationOperators=False):
        data = self.data
        uniqueOperators = data["operator"].unique()
        numberOfOperators = (len(settings.QUERY_ARGS[self.queryName]) -
                             self.ratesPerQuery[self.queryName])
        #print(data.head(5))
        #print(numberOfOperators)

        for operator in uniqueOperators:
            print("SELECTED operator:" + operator)
            operatorDF = data[data["operator"] == operator]
            columnsBeforeInput = 2  # operator + id = 2
            parallelOperators = [
                i for i in range(columnsBeforeInput, columnsBeforeInput +
                                 numberOfOperators)
            ]
            print(parallelOperators)
            # Select all operators
            for parallelColumn in parallelOperators:
                uniqueParallelizationArray = \
                    operatorDF.iloc[:,self.ratesPerQuery[self.queryName] + parallelColumn].unique()
                print(
                    f'{operatorDF.columns[parallelColumn+self.ratesPerQuery[self.queryName]]} == {uniqueParallelizationArray}'
                )
                for parallelIndex, parallelizationValue in enumerate(
                        uniqueParallelizationArray):
                    parallelOperatorName = operatorDF.columns[
                        self.ratesPerQuery[self.queryName] + parallelColumn]
                    df = None

                    # Incrementing only one operator at a time and keep the rest equal to 1
                    print("PARALLELISM:: " + parallelOperatorName + "==" +
                          str(parallelizationValue))

                    df = operatorDF[
                        operatorDF.iloc[:, self.ratesPerQuery[self.queryName] +
                                        parallelColumn] ==
                        parallelizationValue]
                    # for parallelColumn2 in parallelOperators:
                    #     if parallelColumn2 == parallelColumn:
                    #         continue
                    #     parallelOperatorName = operatorDF.columns[
                    #         self.ratesPerQuery[self.queryName] +
                    #         parallelColumn2]
                    #     print("PARALLELISM:: " + str(parallelColumn2) + " :" +
                    #           parallelOperatorName + "== 1")
                    #     df = df[df.iloc[:, self.ratesPerQuery[self.queryName] +
                    #                     parallelColumn2] == 1]

                    rateName = ""

                    for primaryRate in range(
                            columnsBeforeInput, columnsBeforeInput +
                            self.ratesPerQuery[self.queryName]):
                        rateName = df.columns[primaryRate]
                        print("SELECTED RATE:" + rateName)

                        uniqueRates = data.iloc[:, primaryRate].unique()

                        uniqueRates = sorted(uniqueRates)

                        if len(uniqueRates) == 0:
                            print(f"{operator}::  {rateName} NO VALUES?")
                        # uniqueRates = [
                        #     self.queryArg[self.queryName][list(self.queryArg[
                        #         self.queryName].keys())[primaryRate - 1]] * i
                        #     for i in range(1, 11)
                        # ]
                        print()
                        print(uniqueRates)
                        print()

                        uniqueTrueProc = [[] for i in range(len(uniqueRates))]
                        uniqueTrueOut = [[] for i in range(len(uniqueRates))]
                        uniqueObservedOut = [[]
                                             for i in range(len(uniqueRates))]
                        uniqueObservedProc = [[]
                                              for i in range(len(uniqueRates))]

                        for i, rateValue in enumerate(uniqueRates):
                            meanDF = df[df.iloc[:, primaryRate] == rateValue]
                            if meanDF["total_op_instances"].unique()[0] > 1.0:
                                print("Aggregated from: " + str(len(meanDF)))
                                meanDF = self.aggregate_func(
                                    meanDF,
                                    meanDF["total_op_instances"].unique()[0])
                                print(
                                    "Len : " + str(len(meanDF)) + " " +
                                    str(meanDF["total_op_instances"].unique()))
                            # secondRate = meanDF.iloc[:, primaryRate +
                            #                          1 if primaryRate == 1 else primaryRate -
                            #                          1].unique()

                            # Reduce noise - Select the default values for the rest rates
                            # for secondaryRate in range(
                            #         1, self.ratesPerQuery[self.queryName] + 1):
                            #     if secondaryRate == primaryRate:
                            #         continue
                            #     keys = list(
                            #         self.queryArg[self.queryName].keys())
                            #     print("Secondrate selected :" +
                            #           keys[secondaryRate - 1] + "==" +
                            #           str(self.queryArg[self.queryName][keys[
                            #               secondaryRate - 1]]) +
                            #           " len of df: " + str(len(meanDF)))
                            #     meanDF = meanDF[meanDF.iloc[:,
                            #                                 secondaryRate] ==
                            #                     self.queryArg[self.queryName][
                            #                         keys[secondaryRate - 1]]]

                            parallelOperatorName = parallelOperatorName[:6]
                            rateName = rateName[:10]
                            operator = operator[:8] + "OP"
                            uniqueDFtrueProc = meanDF["true_proc_rate"].tolist(
                            )
                            uniqueDFtrueOut = meanDF[
                                "true_output_rate"].tolist()
                            uniqueDFobservedOut = meanDF[
                                "observed_output_rate"].tolist()
                            uniqueDFobservedProc = meanDF[
                                "observed_proc_rate"].tolist()

                            uniqueTrueOut[i].extend(uniqueDFtrueOut)
                            uniqueTrueProc[i].extend(uniqueDFtrueProc)
                            uniqueObservedOut[i].extend(uniqueDFobservedOut)
                            uniqueObservedProc[i].extend(uniqueDFobservedProc)

                        legend = ["v=" + str(i) for i in uniqueRates]
                        self.plot_scatter_ecdf(
                            uniqueTrueOut,
                            "TrueOutput [rec/ut]",
                            f"{operator}__{rateName}_{parallelOperatorName}={parallelizationValue}_TrueOutput",
                            legend=legend)
                        self.plot_scatter_ecdf(
                            uniqueTrueProc,
                            "TrueProcessing [rec/ut]",
                            f"{operator}__{rateName}_{parallelOperatorName}={parallelizationValue}_TrueProcessing",
                            legend=legend)
                        self.plot_scatter_ecdf(
                            uniqueObservedOut,
                            "ObservedOutput [rec/ot]",
                            f"{operator}__{rateName}_{parallelOperatorName}={parallelizationValue}_ObservedOutput",
                            legend=legend)
                        self.plot_scatter_ecdf(
                            uniqueObservedProc,
                            "ObservedProcessing [rec/ot]",
                            f"{operator}__{rateName}_{parallelOperatorName}={parallelizationValue}_ObservedProcessing",
                            legend=legend)
                # All parallelizationOperators are equal so do not loop through again
                if equalParallelizationOperators:
                    break
    def analyzeDistributionParallelism(self):
        print("EQUAL PARALLELISM DISTRIBUTION")
        data = self.data
        uniqueOperators = data["operator"].unique()
        numberOfOperators = (len(settings.QUERY_ARGS[self.queryName]) -
                             self.ratesPerQuery[self.queryName])

        for operator in uniqueOperators:
            print("SELECTED operator:" + operator)
            operatorDF = data[data["operator"] == operator]

            columnsBeforeInput = 2  # operator + id = 2
            parallelOperators = [
                i for i in range(columnsBeforeInput, columnsBeforeInput +
                                 numberOfOperators)
            ]
            uniqueParallelizationArray = \
                operatorDF.iloc[:, self.ratesPerQuery[self.queryName] +parallelOperators[0]].unique()
            uniqueTrueProc = [[]
                              for i in range(len(uniqueParallelizationArray))]
            uniqueTrueOut = [[]
                             for i in range(len(uniqueParallelizationArray))]
            uniqueObservedOut = [
                [] for i in range(len(uniqueParallelizationArray))
            ]
            uniqueObservedProc = [
                [] for i in range(len(uniqueParallelizationArray))
            ]
            for parallelIndex, parallelValue in enumerate(
                    uniqueParallelizationArray):
                df = operatorDF[
                    operatorDF.iloc[:, self.ratesPerQuery[self.queryName] +
                                    parallelOperators[0]] == parallelValue]

                for no in range(1, len(parallelOperators)):
                    df = df[df.iloc[:, self.ratesPerQuery[self.queryName] +
                                    parallelOperators[no]] == parallelValue]
                    parName1 = data.columns[self.ratesPerQuery[self.queryName]
                                            + parallelOperators[no]]
                    print(f'{parName1}#{no} == {parallelValue}')

                parName = data.columns[self.ratesPerQuery[self.queryName] +
                                       parallelOperators[0]]
                print(f'{parName} == {parallelValue}')
                dfNew = None
                dfNew = self.aggregate_func(df, parallelValue)
                print("total instances " +
                      str(dfNew["total_op_instances"].unique()))
                #operator = operator[:8]
                uniqueDFtrueOut = None
                uniqueDFtrueProc = None
                uniqueDFobservedOut = None
                uniqueDFobservedProc = None
                
                uniqueDFtrueProc = dfNew["true_proc_rate"].tolist()
                uniqueDFtrueOut = dfNew["true_output_rate"].tolist()
                uniqueDFobservedOut = dfNew["observed_output_rate"].tolist()
                uniqueDFobservedProc = dfNew["observed_proc_rate"].tolist()
                for i, v in enumerate(uniqueDFtrueOut):
                    if v > 1.5 * 10**7:
                        uniqueDFtrueOut.pop(i)
                uniqueTrueOut[parallelIndex].extend(uniqueDFtrueOut)
                uniqueTrueProc[parallelIndex].extend(uniqueDFtrueProc)
                uniqueObservedOut[parallelIndex].extend(uniqueDFobservedOut)
                uniqueObservedProc[parallelIndex].extend(uniqueDFobservedProc)
                uniqueDFtrueOut = None
                uniqueDFtrueProc = None
                uniqueDFobservedOut = None
                uniqueDFobservedProc = None
                print()

            parallelOperatorName = "Parallelism"
            legend = ["v=" + str(i) for i in uniqueParallelizationArray]

            self.plot_scatter_ecdf(uniqueTrueOut,
                                   "TrueOutput [rec/ut]",
                                   f"{operator}_TrueOutput",
                                   legend=legend,
                                   path="parallelism")
            self.plot_scatter_ecdf(uniqueTrueProc,
                                   "TrueProcessing [rec/ut]",
                                   f"{operator}_TrueProcessing",
                                   legend=legend,
                                   path="parallelism")
            self.plot_scatter_ecdf(uniqueObservedOut,
                                   "ObservedOutput [rec/ot]",
                                   f"{operator}_ObservedOutput",
                                   legend=legend,
                                   path="parallelism")
            self.plot_scatter_ecdf(uniqueObservedProc,
                                   "ObservedProcessing [rec/ot]",
                                   f"{operator}_ObservedProcessing",
                                   legend=legend,
                                   path="parallelism")

    def plot_graph(self,
                   title,
                   x_axis,
                   y_axis,
                   x: List[int],
                   y: List[int],
                   save=True):
        """
        Plot a graph based on x,y arrays
        Store result in PLOT_DIR
        """
        if sum(x) == 0 or sum(y) == 0 or len(x) < 3 or len(y) < 3:
            print(title + str(x) + str(y))
            return None
        plt.clf()
        fig, ax = plt.subplots()

        plt.plot(x, y)

        plt.xlabel(x_axis)
        plt.ylabel(y_axis)

        plt.title(title)
        plt.tight_layout()
        if save == True:
            path = settings.PLOT_DIR + self.queryName + "_rate/"
            self.check_path(path)
            plt.savefig(path + self.checkStr(title) + ".png")
        else:
            plt.show()
        plt.close(fig)

    def multiple_lines_plot(self,
                            title,
                            x_axis,
                            y_axis,
                            x: List[List[int]],
                            y: List[List[int]],
                            save=True):
        """
        Plot a graph based on x,y List of lists
        Each line represented by x-y sublists are on the same plot.
        Store result in PLOT_DIR
        """
        fig, ax = plt.subplots()

        if len(x) != len(y):
            print("x and y must be same length")
            return
        for n in range(len(x)):
            if sum(x[n]) == 0 or sum(y[n]) == 0:
                return
            plt.plot(x[n], y[n])

        plt.xlabel(x_axis)
        plt.ylabel(y_axis)
        plt.legend(["Value = 1", "V = 2", "V = 3"], loc="lower right")
        plt.title(title)
        plt.tight_layout()
        if save == True:
            path = settings.PLOT_DIR + self.queryName + "/" + self.CSV_SPECIFIER + "/"
            self.check_path(path)
            plt.savefig(path + self.checkStr(title) + ".png")
        else:
            plt.show()
        plt.close()

    def plot_scatter_ecdf(
            self,
            data,
            xlabel,
            title,
            legend=["Parallelism = 1", "P = 2", "P = 3", "P = 4"],
            scatter_size=5,
            multiple=True,
            path=""):
        def ecdf(data):
            """ Compute ECDF """
            #print(type(data))
            x, n = None, 0
            if isinstance(data, list):
                #print(data)
                x = sorted(data)
                n = len(x)
                if sum(x) == 0 or n < 5:
                    return None, None
            elif isinstance(data, np.ndarray):
                x = np.sort(data)
                n = x.size
                if np.sum(x) == 0 or n < 5:
                    return None, None
            else:
                print(data)
                print(type(data))
                print("NOT NUMPY")
                return None, None
            y = np.arange(1, n + 1) / n
            return (x, y)

        if multiple:
            for subplotX in data:
                xsub, ysub = ecdf(subplotX)
                if xsub is not None and ysub is not None:
                    plt.scatter(x=xsub, y=ysub, s=scatter_size)
                else:
                    print(self.checkStr(title) + " is EMPTY")
                    return
        else:
            x, y = ecdf(data)
            if x is not None and y is not None:
                print(x, y)
                plt.scatter(x=x, y=y, s=scatter_size)
            else:
                return
        # if path != "":
        #     path = "_" + path
        plt.xlabel(xlabel)
        plt.ylabel("Percentage (%)")
        plt.legend(legend, loc="lower right")
        plt.title(title)
        plt.tight_layout()
        path = settings.PLOT_DIR + self.queryName + "/" + self.CSV_SPECIFIER + "/scatter" + path + "/"
        self.check_path(path)
        plt.savefig(path + self.checkStr(title) + ".png")
        plt.close()

    def checkStr(self, title: str) -> str:
        """ Removes any non alphanumerical character from input string."""
        return re.sub(r"\W+", "", title)

    def check_path(self, path):
        """ Checks if given path is valid otherwise create it"""
        if not os.path.exists(path):
            os.makedirs(path)


def csv_unifier():
    """Takes _metrics.csv and _equalmetrics.csv and combines them to one"""
    valid_query_names = settings.VALID_QUERIES_NAMES
    PATH = settings.PATH
    specifiers = settings.VALID_CSV_SPECIFIERS
    for query in valid_query_names:
        for speficier in specifiers:
            filepath = PATH + query + "_" + speficier + ".csv"
            if os.path.exists(filepath):
                print('reading ' + filepath)
                fin = open(filepath, "r")
                dataRead = fin.read()
                fin.close()
                fout = open(PATH + query + "_all.csv", 'a+')
                fout.write(dataRead)
                fout.close()
            else:
                print(filepath + " not found")


if __name__ == "__main__":
    args = sys.argv
    if len(args) == 1:
        print("No arguments at " + args[0] + " exiting...")
        exit(-1)

    valid_query_names = settings.VALID_QUERIES_NAMES

    if args[1] == 'combine':
        csv_unifier()
        exit(1)

    if args[1] in valid_query_names or args[1] == "all":
            Analyzer(queryName=args[1],
                     specifier=args[2] if len(args) >= 3 else "",
                     action=args[3] if len(args) == 4 else None)
    else:
        print("Argument [" + args[1] + "] is invalid. Exiting " + args[0])
