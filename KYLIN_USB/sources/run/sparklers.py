import pandas as pd
import numpy as np
import time
from subprocess import Popen


import sys
import os
sys.path.append(os.path.abspath("/home/hadoop/KYLIN_USB/sources/run/*"))

import global_variables as gv


class sparker(object):
    # Initializer / Instance Attributes
    def __init__(self, name, path, log_path, conf_path):
        self.name = name
        self.path = path
        self.conf = conf_path
        self.logs = log_path
        self.yarn = 'To be launched'
        self.cmd = []
        self.priority = None
        self.process = None
        self.process_md = None
        self.arg1 = ''
        self.arg2 = ''

    # get spark-submit commands with the conf indicated to the sparkle
    def get_cmd(self):
        f = open(self.conf)
        cmd_list = ["spark-submit"]
        for line in f:
            cmd_list.append(line.rstrip('\n'))
        cmd_list.append(self.path)
        self.cmd = cmd_list

    def prepare_submit(self, conf, intervals):
        # get conf path
        self.conf = conf
        # if it's a refresh we need to get the interval to prepare
        year = intervals[0]
        month = intervals[1]
        # get the spark-submit command for logs
        self.get_cmd()
        # create the log directory for this sparker
        if not gv.os.path.exists(self.logs):
            gv.os.makedirs(self.logs)
        if (self.path.__contains__('001-f_transaction_detail.py') or self.path.__contains__(
                'etapes_de_vie.py') or self.path.__contains__('kyl_mqb_vte.py')):
            # append year of instant
            self.cmd.append(intervals[0])
            self.arg1 = intervals[0]
            # append month of instant
            self.cmd.append(intervals[1])
            self.arg2 = intervals[1]

    def spark_submit(self):
        try:
            stderr_file = open(self.logs + 'stderr', 'wb')
            stdout_file = open(self.logs + 'stdout', 'wb')
            p = Popen(self.cmd, stderr=stderr_file, stdout=stdout_file, bufsize=1)
            # p.communicate()
            self.process = p
        except ValueError:
            print("Failed Spark-submit with command : " + str(self.cmd))


class MasterSparkles(object):
    # init
    def __init__(self, facts, masterdata):
        self.facts = facts
        self.masterdata = masterdata
        self.registry = pd.DataFrame(columns=["Name", "Priority", "Sparkle"])
        self.refresh = False

    # append to registry
    def append_to_registry(self, df):
        if len(df.columns) == len(self.registry.columns):
            self.registry = self.registry.append(df)
        else:
            print("Registry and the DataFrame you want to append do not have the same number of columns")

    # get registry
    def get_register(self):
        return self.registry

    # init registry
    def init_registry(self):
        self.registry = pd.DataFrame(columns=["Name", "Priority", "Sparkle"])

    # dump registry to csv
    def dump_registry(self):
        pass

    # get all the month/year intervalle from 2017-01-01 until now
    @staticmethod
    def get_intervals():
        intervals = [[format(gv.start.year, "04"), format(gv.start.month, "02")]]
        loop_time = gv.start
        while loop_time < gv.time:
            loop_time = loop_time + gv.delta
            if loop_time < gv.time - gv.delta:
                intervals.append([format(loop_time.year, "04"), format(loop_time.month, "02")])
        return intervals

    @staticmethod
    def get_actual_interval():
        return [format(gv.time.year, "04"), format(gv.time.month, "02")]

    # define object priority
    @staticmethod
    def get_priority(path):
        if path.__contains__('load/facts'):
            return 0
        elif path.__contains__('load/sources'):
            return 1
        elif path.__contains__('load/transform'):
            return 2
        elif path.__contains__('load/hierarchies'):
            return 3
        elif path.__contains__('load/dpp'):
            return 4
        elif path.__contains__('load/cubes'):
            return 5
        else:
            print("Sparkle can't get a priority of execution !")
            return -1

    # prepare registry for increment data preparation
    def data_prep_increment(self):
        # add path to tables
        a = [gv.PLfacts + s for s in gv.Tfacts]
        b = [gv.PLtransform + s for s in gv.Ttransform]
        c = [gv.PLcubes + s for s in gv.Tcubes]
        d = [gv.PLsources + s for s in gv.Tsources]
        tables = a + b + c + d
        # Prepare registry
        for i in enumerate(tables):
            # instantiate a sparkle
            sparkle = sparker(name="Sparkle_" + str(i[0]), path=i[1],
                              log_path=gv.log_path + "Sparkle_" + str(i[0]) + "/", conf_path=gv.spark_conf_path)
            # prepare the sparkle to be submitted like an increment
            sparkle.prepare_submit(sparkle.conf + 'default_spark_conf.txt', self.get_actual_interval())
            # attach priority to sparkle
            sparkle.priority = self.get_priority(sparkle.path)
            # append the sparkle to the master registry
            df = pd.DataFrame([[sparkle.name, sparkle.priority, sparkle]], columns=["Name", "Priority", "Sparkle"])
            self.append_to_registry(df)
        # Resetting index
        self.registry = self.registry.reset_index(drop=True)
        # We need to have unique Sparkles
        self.registry = self.registry.drop_duplicates(subset=['Name'])

    # prepare registry for refresh data preparation (basically we need to refresh all the data
    def data_prep_refresh(self):
        # add path tables:
        a = [gv.PLfacts + s for s in gv.Tfacts]
        b = [gv.PLsources + s for s in gv.Tsources]
        c = [gv.PLtransform + s for s in gv.Ttransform]
        d = [gv.PLhierarchies + s for s in gv.Thierarchies]
        e = [gv.PLcubes + s for s in gv.Tcubes]
        f = [gv.PLdpp + s for s in gv.Tdpp]
        # concat tables paths
        tables = a + b + c + d + e + f
        # prepare classic registry
        for i in enumerate(tables):
            sparkle = sparker(name="Sparkle_" + str(i[0]), path=i[1],
                              log_path=gv.log_path + "Sparkle_" + str(i[0]) + "/", conf_path=gv.spark_conf_path)
            sparkle.prepare_submit(sparkle.conf + 'default_spark_conf.txt', self.get_actual_interval())
            # attach priority to sparkle
            sparkle.priority = self.get_priority(sparkle.path)
            # append the sparkle to the master registry
            df = pd.DataFrame([[sparkle.name, sparkle.priority, sparkle]], columns=["Name", "Priority", "Sparkle"])
            self.append_to_registry(df)
        # add history to registry
        cpt = len(self.registry)
        for i in self.get_intervals():
            # adding the steps life of history
            sparkle0 = sparker(name="Sparkle_" + str(cpt), path=gv.PLfacts + '001-f_transaction_detail.py',
                               log_path=gv.log_path + "Sparkle_" + str(cpt) + "/", conf_path=gv.spark_conf_path)
            sparkle0.prepare_submit(sparkle0.conf + 'default_spark_conf.txt', i)
            # attach priority to sparkle
            sparkle0.priority = self.get_priority(sparkle0.path)
            df0 = pd.DataFrame([[sparkle0.name, sparkle0.priority, sparkle0]], columns=["Name", "Priority", "Sparkle"])
            cpt = cpt + 1
            # adding the steps life of history
            sparkle1 = sparker(name="Sparkle_" + str(cpt), path=gv.PLtransform + 'etapes_de_vie.py',
                               log_path=gv.log_path + "Sparkle_" + str(cpt) + "/", conf_path=gv.spark_conf_path)
            sparkle1.prepare_submit(sparkle1.conf + 'default_spark_conf.txt', i)
            # attach priority to sparkle
            sparkle1.priority = self.get_priority(sparkle1.path)
            df1 = pd.DataFrame([[sparkle1.name, sparkle1.priority, sparkle1]], columns=["Name", "Priority", "Sparkle"])
            cpt = cpt + 1
            # adding the sales facts table to history
            sparkle2 = sparker(name="Sparkle_" + str(cpt), path=gv.PLcubes + 'kyl_mqb_vte.py',
                               log_path=gv.log_path + "Sparkle_" + str(cpt) + "/", conf_path=gv.spark_conf_path)
            sparkle2.prepare_submit(sparkle2.conf + 'default_spark_conf.txt', i)
            # attach priority to sparkle
            sparkle2.priority = self.get_priority(sparkle2.path)
            df2 = pd.DataFrame([[sparkle2.name, sparkle2.priority, sparkle2]], columns=["Name", "Priority", "Sparkle"])
            cpt = cpt + 1
            df = df0.append(df1).append(df2)
            self.append_to_registry(df)

        # Resetting index
        self.registry = self.registry.reset_index(drop=True)
        # We need to have unique Sparkles
        self.registry = self.registry.drop_duplicates(subset=['Name'])


class spark_pool(object):
    def __init__(self, name, size):
        self.name = name
        self.size = size
        self.content = np.empty(size, dtype=object)
        self.cpt = 0

    def full(self):
        self.cpt = 0
        for i in range(self.size):
            if self.content[i] is not None:
                self.cpt = self.cpt + 1
        if self.cpt == self.size:
            self.cpt = 0
            return True
        else:
            self.cpt = 0
            return False

    def empty(self):
        self.cpt = 0
        for i in range(self.size):
            if self.content[i] is not None:
                self.cpt = self.cpt + 1
        if self.cpt == 0:
            self.cpt = 0
            return True
        else:
            self.cpt = 0
            return False

    def put(self, item, index):
        print("Adding " + item.name + " to spark pool | executing : " + item.path.split('/')[
            -1] + " " + item.arg1 + " " + item.arg2)
        self.content[index] = item

    def remove(self, index):
        print("Removing " + self.content[index].name + " from spark pool")
        self.content[index] = None

    def get_space(self):
        self.cpt = 0
        for i in range(self.size):
            if self.content[i] is None:
                self.cpt = self.cpt + 1
        if self.cpt < self.size:
            self.cpt = 0
            return True
        else:
            self.cpt = 0
            return False


class executor(spark_pool):
    def __init__(self, name, size):
        super().__init__(name, size)

    def sparkle_to_compute(self, registry):
        flag = False
        for j in range(len(registry)):
            item = registry['Sparkle'].loc[j]
            if item.yarn == 'To be launched':
                flag = True
        return flag

    def log_pool(self):
        for i in range(len(self.content)):
            item = self.content[i]
            if item is not None:
                print("Index :" + str(i) + "|" + "Name :" + str(item.name) + "|" + "Yarn :" + str(
                    item.yarn) + "|" + "RC :" + str(item.process.poll()))

    def put_in_pool(self, registry):
        open_space = None
        sparkle = None
        # find a space in the pool
        for i in range(self.size):
            if self.content[i] is None:
                open_space = i
                break
        # find a non-treated sparkle
        for i in range(len(registry)):
            item = registry['Sparkle'].loc[i]
            if item.yarn == 'To be launched':
                sparkle = item
                break
        # add the sparkle to the open space and executing it
        if open_space is not None and sparkle is not None:
            self.put(sparkle, open_space)
            sparkle.spark_submit()
            sparkle.yarn = 'Launched'

    def remove_from_pool(self):
        sparkle_removed = False
        # find a space where the treatment is finished
        for i in range(self.size):
            if self.content[i] is not None:
                if self.content[i].yarn == 'Launched' and self.content[i].process.poll() is not None:
                    self.content[i].yarn = self.get_yarn_status()
                    self.remove(i)
                    sparkle_removed = True
                else:
                    continue
            # space is open, nothing to do here
            else:
                continue
        return sparkle_removed

    def launch_tables(self, index):
        MetaFacts = gv.PMfacts + 'facts.hql'
        MetaSource = gv.PMsources + 'sources.hql'
        MetaTransform = gv.PMtransform + 'transform.hql'
        MetaHierarchies = gv.PMhierarchies + 'hierarchies.hql'
        MetaDpp = gv.PMdpp + 'dpp_hierarchies.hql'
        MetaCubes = gv.PMcubes + 'cubes.hql'
        Meta = [MetaFacts, MetaSource, MetaTransform, MetaHierarchies, MetaDpp, MetaCubes]
        cmd = ['beeline', '-u', 'jdbc:hive2://localhost:10000', '-n', 'hadoop', '-f', Meta[index]]
        print("Create table with cmd:" + str(cmd))
        try:
            metadata_err_file = open('/home/hadoop/' + 'metadataerr', 'wb')
            metadata_out_file = open('/home/hadoop/' + 'metadataout', 'wb')
            t = Popen(cmd, stderr=metadata_err_file, stdout=metadata_out_file, bufsize=1)
            t.wait()
            # p.communicate()
            # self.process_md = t
        except ValueError:
            print("Failed create table with command : " + str(cmd))

    # TODO
    @staticmethod
    def get_yarn_status():
        return 'SUCCEEDED'

    def write_to_csv():
        pass

    def launch(self, registry):
        priorities = registry['Priority'].drop_duplicates().array.sort()
        for i in priorities:
            print("Executing Priority : " + str(i))
            df = registry[registry.Priority == i].reset_index(drop=True)
            # if we have something to compute
            while self.sparkle_to_compute(df):
                if self.full():
                    is_removed = self.remove_from_pool()
                    if is_removed:
                        continue
                    else:
                        print("pool is full and there is no sparkle to remove")
                        time.sleep(5)
                else:
                    self.put_in_pool(df)
            # while all the process do not have been treated
            while not self.empty():
                is_removed = self.remove_from_pool()
                if is_removed:
                    continue
                else:
                    print("There is no more spark treatment for this priority, waiting process to finish")
                    self.log_pool()
                    time.sleep(5)
            # if everything has been treated we create the table of this priority
            if self.empty():
                self.launch_tables(i)
