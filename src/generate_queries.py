#!/usr/bin/env python3
import timeit
import time
import os
import sys
import copy
import subprocess
from typing import List
import settings


class QueryGenerator:
    """
    max value for every operator's parallelism
    """
    MAX_PARALLELISM = settings.MAX_PARALLELISM
    """
    Amount of iterations per rate value, if run_query_rates() is selected
    """
    RATE_ITERATIONS = settings.RATE_ITERATIONS

    query_script_path = settings.SCRIPT_PATH
    rates_path = settings.MONITOR_DIRECTORY

    # Depicts how many rates each Query has.
    ratesPerQuery = settings.RATES_PER_QUERY

    # increment Rates of each argument
    incrementMetrics = copy.deepcopy(settings.INCREMENT_METRICS)

    queryRates = settings.RATES_NAMES_PER_QUERY

    queryOperators = settings.OPERATORS_NAMES_PER_QUERY

    queryArg = copy.deepcopy(settings.QUERY_ARGS)

    def __init__(self, queryName, extension):
        self.queryName = queryName
        self.extension = extension
        # Make sure /rates/ is empty so on_create routine detects file creation.
        self.update_metrics_dic()
        self.clear_results()
        if not os.path.exists(self.rates_path):
            os.makedirs(self.rates_path)

    def inc_specific_rate(self, queryName, n):
        """ WARNING: Python3.6+ is required that has ordered-insertion dictionary built-in.
        Increment the n-th rate of the given @queryName.
        Increment rates are given by the self.incrementMetrics dictionary.
        """
        if n not in range(self.ratesPerQuery[queryName]):
            return
        self.queryArg[queryName][list(self.queryArg[queryName])[n]] += \
            self.incrementMetrics[queryName][list(self.incrementMetrics[queryName])[n]]

        if isinstance(
                self.queryArg[queryName][list(self.queryArg[queryName])[n]],
                float):
            self.queryArg[queryName][list(self.queryArg[queryName])[n]] = \
                round(self.queryArg[queryName][list(self.queryArg[queryName])[n]], 2)

    def reset_specific_rate(self, queryName, n):
        """ WARNING: Python3.6+ is required that has ordered-insertion dictionary built-in.
        Reset all the rates till the n-th rate of the given @queryName.
        """
        for i in range(n):
            self.queryArg[queryName][list(
                self.queryArg[queryName])[i]] = settings.QUERY_ARGS[queryName][
                    list(settings.QUERY_ARGS[queryName])[i]]

    def run_query_rates(self, queryName, reset=True):
        """Runs @queryName scripts with contantly increasing rates at a time.
        Increase each rate for the given query RATE_ITERATIONS times.
        """
        for i in range(self.RATE_ITERATIONS * self.ratesPerQuery[queryName] +
                       1):
            print(queryName + "::" + self.argStringPerQuery[queryName])

            start = timeit.default_timer()
            subprocess.call([
                self.query_script_path, queryName,
                self.argStringPerQuery[queryName], self.extension
            ])
            end = timeit.default_timer()
            self.saveTime(end - start)
            if (i >= self.RATE_ITERATIONS
                    and i % self.RATE_ITERATIONS == 0):  # and reset:
                self.reset_specific_rate(queryName,
                                         i // (self.RATE_ITERATIONS))
            self.inc_specific_rate(queryName, i // (self.RATE_ITERATIONS))
            self.update_metrics_dic()
            if reset:
                self.clear_results()
        if reset:
            self.reset_metrics()

    def run_query(self, queryName, allOperators=False):
        """Runs @queryName scripts with contantly increasing arguments (including rates and operators parellelization)."""
        metricsNumberPerQuery = (len(self.queryArg[queryName]) -
                                 self.ratesPerQuery[queryName])
        # -1 in order to skip last run with value == 4 as previous runs did not have 4
        for i in range(1, self.MAX_PARALLELISM * metricsNumberPerQuery - 1):
            # -- SLOPPY FIX --
            # skip duplicate arguments of values: 1-4-1 happening on queries with 3 parallelism operators
            # if i == 8:
            #     continue
            if i >= self.MAX_PARALLELISM and i % self.MAX_PARALLELISM == 0:
                if allOperators:
                    break
                self.reset_metrics()
                # do not run with parallization value = 1.
                self.increment_specific_operator(queryName,
                                                 i // self.MAX_PARALLELISM)
                self.update_metrics_dic()
                continue
            self.run_query_rates(queryName, False)
            if allOperators:
                self.increment_all_operator(queryName)
            else:
                self.increment_specific_operator(queryName,
                                                 i // self.MAX_PARALLELISM)
            self.update_metrics_dic()
            self.clear_results()
        self.reset_metrics()

    def saveTime(self, time):
        minutes = round(time / 60, 2)
        line = ','.join(
            map(
                lambda x: str(x),
                list(self.queryArg[self.queryName].values()),
            )) + "," + str(minutes)
        path = settings.PATH + self.queryName + "_time.txt"
        with open(path, 'a+') as fp:
            fp.write(line)
            print("stored to " + path + "::" + line)

    def increment_specific_operator(self, queryName, operatorIndex):
        """
        Increase the parellilazation of a specific operator of the given queryName
        """
        operator = self.queryOperators[queryName][operatorIndex]
        if self.queryArg[queryName][operator] >= self.MAX_PARALLELISM:
            self.queryArg[queryName][operator] = self.MAX_PARALLELISM
        else:
            self.queryArg[queryName][operator] = min(
                self.queryArg[queryName][operator] +
                self.incrementMetrics[queryName][operator],
                self.MAX_PARALLELISM,
            )
        self.update_metrics_dic()

    def increment_all_operator(self, queryName):
        """
        Increase the parellilazation of a specific operator of the given queryName
        """
        for operator in self.queryOperators[queryName]:
            if self.queryArg[queryName][operator] >= self.MAX_PARALLELISM:
                self.queryArg[queryName][operator] = self.MAX_PARALLELISM
            else:
                self.queryArg[queryName][operator] = min(
                    self.queryArg[queryName][operator] +
                    self.incrementMetrics[queryName][operator],
                    self.MAX_PARALLELISM,
                )
            self.update_metrics_dic()

    def increment_rates(self, queryName):
        """
        Increase only the source rates related to given queryName and update the argument string
        """
        breakPoint = self.ratesPerQuery[queryName]
        counter = 0
        for metric in self.queryArg[queryName]:
            self.queryArg[queryName][metric] += self.incrementMetrics[
                queryName][metric]
            counter += 1
            if counter >= breakPoint:
                break
        self.update_metrics_dic()

    def increment_metrics(self, queryName):
        """
        Increase all the metrics related to the given queryName
        """
        for metric in self.queryArg[queryName]:
            if self.queryArg[queryName][metric] >= self.MAX_PARALLELISM:
                self.queryArg[queryName][metric] = self.MAX_PARALLELISM
            else:
                self.queryArg[queryName][metric] = min(
                    self.queryArg[queryName][metric] +
                    self.incrementMetrics[queryName][metric],
                    self.MAX_PARALLELISM,
                )
        self.update_metrics_dic()

    def reset_metrics(self):
        """
        Reset to default values all the metrics related to all queries' arguments
        """
        self.queryArg = copy.deepcopy(settings.QUERY_ARGS)
        self.update_metrics_dic()

    def clear_results(self):
        """
        Dedicated function to remove all .log files under 'rates_path' directory
        """
        os.system("rm " + self.rates_path + "*.log")
        print(self.rates_path + " directory is now empty. Running 'ls':")
        os.system("ls " + self.rates_path)
        return

    def update_metrics_dic(self):
        """
        Update string arguments with the updated query Arguments
        """
        self.argStringPerQuery = {
            "Query1":
            f'exRate,{str(self.queryArg["Query1"]["exRate"])}#srcRate,{str(self.queryArg["Query1"]["srcRate"])}'
            f'#psource,{str(self.queryArg["Query1"]["psource"])}#pmap,{str(self.queryArg["Query1"]["pmap"])}',
            "Query2":
            f'srcRate,{str(self.queryArg["Query2"]["srcRate"])}'
            f'#psource,{str(self.queryArg["Query2"]["psource"])}#pmap,{str(self.queryArg["Query2"]["pmap"])}',
            "Query3":
            f'auctionRate,{str(self.queryArg["Query3"]["auctionRate"])}#personRate,{str(self.queryArg["Query3"]["personRate"])}'
            f'#pAuctionSource,{str(self.queryArg["Query3"]["pAuctionSource"])}#pPersonSource,{str(self.queryArg["Query3"]["pPersonSource"])}#pjoin,{str(self.queryArg["Query3"]["pjoin"])}',
            "Query3Stateful":
            f'auctionRate,{str(self.queryArg["Query3Stateful"]["auctionRate"])}#personRate,{str(self.queryArg["Query3Stateful"]["personRate"])}'
            f'#pAuctionSource,{str(self.queryArg["Query3Stateful"]["pAuctionSource"])}#pPersonSource,{str(self.queryArg["Query3Stateful"]["pPersonSource"])}#pjoin,{str(self.queryArg["Query3Stateful"]["pjoin"])}',
            "Query5":
            f'srcRate,{str(self.queryArg["Query5"]["srcRate"])}'
            f'#p-bid-source,{str(self.queryArg["Query5"]["psource"])}#p-window,{str(self.queryArg["Query5"]["pmap"])}',
            "Query8":
            f'auctionRate,{str(self.queryArg["Query8"]["auctionRate"])}#personRate,{str(self.queryArg["Query8"]["personRate"])}'
            f'#pAuctionSource,{str(self.queryArg["Query8"]["pAuctionSource"])}#pPersonSource,{str(self.queryArg["Query8"]["pPersonSource"])}#pjoin,{str(self.queryArg["Query8"]["pjoin"])}',
            "Query11":
            f'srcRate,{str(self.queryArg["Query11"]["srcRate"])}'
            f'#p-bid-source,{str(self.queryArg["Query11"]["psource"])}#p-window,{str(self.queryArg["Query11"]["pmap"])}',
        }


if __name__ == "__main__":
    args = sys.argv
    if len(args) == 1:
        print("No arguments at " + args[0] + " exiting...")
        exit(-1)

    if args[1] == "clear":
        qs = QueryGenerator("", "")
        qs.clear_results()
        exit(1)

    valid_query_names = settings.VALID_QUERIES_NAMES
    # valid_second_arg = settings.VALID_CSV_SPECIFIERS

    # if args[2] not in valid_second_arg:
    #     print("Second argument must be " + str(valid_second_arg) +
    #           " and not " + args[2] + "." + args[0] + " exiting...")
    #     exit(-1)

    if args[1] in valid_query_names or args[1] == "all":
        qs = QueryGenerator(args[1], args[2])
        if args[1] == "all":
            for q in valid_query_names:
                if args[2] == "rates":
                    qs.run_query_rates(q)
                elif args[2] == "metrics":
                    qs.run_query(q)
        else:
            if args[2] == "rates":
                # generate  queries with only different rates (all Parallelism == 1).
                qs.run_query_rates(args[1])
            elif "equal" in args[2]:
                # generate queries in which all parallelism values increase the same.
                qs.run_query(args[1], True)
            else:
                # generate queries in which by each step you increase 1 input argument.
                qs.run_query(args[1])
    else:
        print("Argument [" + args[1] + "] is invalid. Exiting " + args[0])
