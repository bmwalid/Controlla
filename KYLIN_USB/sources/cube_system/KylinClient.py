

# !/usr/bin/python
import itertools
import json
import subprocess
from time import sleep, time
import requests
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool as ProcessPool
from datetime import datetime

from KYLIN_USB.sources.cube_system.global_variables import KylinProperties, LoggerProperties
from KYLIN_USB.sources.cube_system.script_logger import get_logger

LOGGER = get_logger(__name__)


# TODO: append json files to the same log file
def build_parallelized(parallelized_cubes, size_pool=5, multiprocessing=True):
    """
    :param parallelized_cubes: a list of tuples of type (cube_name,segment)
    e.g. [("MODEL_MQB_DEF_DMI","20170101000000_20181101000000"),("MODEL_MQB_DEF_DPP","20170101000000_20181101000000")]
    :param size_pool: size of the pool containing the active threads. 0 default means maximum
    :param multiprocessing: if we want to use multiprocessing or multithreading
    :return:
    """
    LOGGER.debug("the list of cubes for build parallelized are : " + parallelized_cubes)
    if parallelized_cubes:
        if multiprocessing:
            pool = ProcessPool(size_pool)
        else:
            pool = ThreadPool(size_pool)
        cube_names_list = list()
        segments_list = list()
        for cubes, segments in parallelized_cubes:
            cube_names_list.append(cubes)
            segments_list.append(segments)
        results = pool.map(incremental_build_star, itertools.izip(segments_list, cube_names_list))
        pool.close()
        pool.join()
    else:
        LOGGER.warn("list of the cubes for a parallelized cubes empty")
        return list()
    return results


def incremental_build_star(a_b):
    return incremental_build(*a_b)


def script_from_df(_path_csv="df_avant_cubes.csv", size_pool=5, lookup_enabled=False):
    """
    :param _path_csv: the csv containing metadata dataframe about building the cubes
    :param size_pool: size of the pool containing the active threads. 0 default means maximum
    :return:
    """
    LOGGER.info("reading metadata of the script from " + _path_csv)
    df = pd.read_csv(_path_csv)
    # list containing the cubes to build and refresh
    build_cubes_parallelized = list()
    build_cubes_sequential = list()
    refresh_cubes_parallelized = list()
    refresh_cubes_sequential = list()

    for row in df.iterrows():
        # create the list of cubes to refresh
        # we append a tuple containing (name of the cube, list of its segments)
        if row["Operation"].lower() == "refresh":
            if "VTE" in row["name"]:
                refresh_cubes_parallelized.append((row["name"], row["segments"]))
            else:
                refresh_cubes_sequential.append((row["name"], row["segments"]))
        # create the listt of cubes to do an incremental build
        elif "increment" in row["Operation"].lower():
            if "VTE" in row["name"]:
                build_cubes_parallelized.append((row["name"], row["segments"]))
            else:
                build_cubes_sequential.append((row["name"], row["segments"]))
        if lookup_enabled:
            if row["lookup"] == "FAUX":
                LOGGER.info("refreshing lookup tables")
                refresh_lookup_tables(row["segments"], row["name"])

    LOGGER.info("launching refreshing cubes in parallel")
    refresh_parallelized(refresh_cubes_parallelized, size_pool)
    LOGGER.info("parallelized refreshing complete")
    for cube_struct_refresh in refresh_cubes_sequential:
        refresh_from_df(cube_struct_refresh[1], cube_struct_refresh[0])

    build_parallelized(build_cubes_parallelized, size_pool)
    for cube_struct_build in build_cubes_sequential:
        incremental_build(cube_struct_build[1], cube_struct_build[0])


def latest_date(list_segments):
    """
    :param list_segments: a list of segments e.g.
    ["20120101000000_20120601000000","20120601000000_20121201000000"]
    :return: the latest segment
    """
    # create a list of tuples
    # The tuple contains (the date of the end date of a segment converted to timestamp, the segment)
    segments_by_end_date = [(time_to_unix(segment.split("_")[1]), segment) for segment in list_segments]
    segments_by_end_date.sort(key=lambda x: x[0], reverse=True)
    return segments_by_end_date[0][1]


def refresh_from_df(segments_list, cube_name):
    final_segment = merge_all_reduce(segments_list, cube_name)
    return refresh(cube_name, final_segment)


def refresh_from_df_star(a_b):
    return refresh_from_df(*a_b)


def refresh_with_purge_parallelized(cubes_list, size_pool=5, multiprocessing=True):
    """

    :param cubes_list: a list containing tuples. Each tuple contains the cube name and its list of semgents
    :param size_pool: the number of running threads
    :param multiprocessing: to use multiprocessing or multithreading
    :return: a list of segments of each cube refreshed
    """
    LOGGER.debug("list of cubes for parallelized refresh are " + cubes_list)
    if cubes_list:
        if multiprocessing:
            pool = ProcessPool(size_pool)
        else:
            pool = ThreadPool(size_pool)
        cube_names_list = list()
        segments_list = list()
        for cubes, segments in cubes_list:
            cube_names_list.append(cubes)
            segments_list.append(segments)
        final_segments = pool.map(refresh_with_purge_star, itertools.izip(cube_names_list, segments_list))
        pool.close()
        pool.join()
    else:
        LOGGER.warn("list of cubes to be refreshed in parallel is empty")
        return list()
    return final_segments


def refresh_with_purge_star(a_b):
    return refresh_with_purge(*a_b)


def refresh_parallelized(list_cubes, size_pool=5, multiprocessing=True):
    if list_cubes:
        if multiprocessing:
            pool = ProcessPool(size_pool)
        else:
            pool = ThreadPool(size_pool)
        cubes_list = list()
        segments_list = list()
        for cubes, segments in list_cubes:
            cubes_list.append(cubes)
            segments_list.append(segments)
        pool.map(refresh_star, itertools.izip())


def incremental_build(segment_list, cube_name):
    """
    Build a new segment of a cube.
    :param segment:
    :return: a dictionary containing the response of merging the new segment with the
    old big segment
    """
    if segment_list:
        return launch_build(time_to_unix(latest_date(segment_list).split('_')[1]), int(time.time()) * 1000, cube_name)
    else:
        return launch_build(time_to_unix("20170101000000"), int(time.time()) * 1000, cube_name)



def check_job_completed(response_build):
    """
    :param response_build: a string containing the body of the json returned by a build, merge or refresh instruction
    :return: status of the job at the end of operation
    """
    job_id = json.loads(response_build)["uuid"]
    url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/jobs"
    response_check = requests.get(url=url, headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
    check_dict = json.loads(response_check.text)
    for e in check_dict:
        if e["uuid"] == job_id:
            job_status = e["job_status"]
            break
    while job_status == "RUNNING" or job_status == "PENDING":
        response_check = requests.get(url=url, headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
        check_dict = json.loads(response_check.text)
        for e in check_dict:
            if e["uuid"] == job_id:
                job_status = e["job_status"]
                break
        sleep(1)
        # check if the build is finished
    return job_status


def get_segments():
    """
    :return: return a dataframe containing all information about all segments.
    There might segments which are not functional
    the dataframe is stored in segments.csv
    """
    list_rows = list()
    LOGGER.debug("getting the list of all jobs")
    url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/jobs"
    response = requests.get(url=url, headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
    for job in json.load(response.text.encode('utf-8')):
        row = dict()
        job_complete_name = job["name"].split('-')
        row["cube_name"] = job_complete_name[0].strip().encode('utf - 8')
        segment_id = job_complete_name[1].strip().encode('utf - 8')
        row["segment_id"] = segment_id
        row["start_date"] = segment_id.split('_')[0]
        row["end_date"] = segment_id.split('_')[1]
        list_rows.append(row)
    df = pd.DataFrame(list_rows)
    LOGGER("all segments information are stored in file ./segments.csv")
    df.to_csv("segments.csv", sep=";")
    return df


def get_applicationid(log_):
    """
    :param log_: the string containing all information about the spark-submit job log
    :return: the id of the application of spark-submit
    """
    application = ""
    for line in log_:
        # find the line where it shows the location of the application log
        if "hdfs:/var/log/spark/apps/application" in line:
            application = line.split(' ')
            break
    # case we didn't file the application id in the log file
    if application == "":
        return ""

    application[-1] = application[-1].split('_')

    application[-1][-1] = application[-1][-1].strip()

    # to get the folder that contains spark logs
    # path_log = application[-1][0]
    return application[-1][1] + "_" + application[-1][2]


# Allow to launch spark_submit

def launch_spark_submit(path, conf_job_file=""):
    """
    :param path: path of the spark job .py
    :param conf_job_file: a file that contains the conf to launch the spark job. If not specified
    it will use the default conf in cong_environment variable
    :return:
    """
    # default conf
    conf_environment = {
        "spark.dynamicAllocation.enabled": "true",
        "spark.executor.instances": "2",
        "spark.dynamicAllocation.minExecutors": "2",
        "spark.dynamicAllocation.maxExecutors": "512",
        "spark.yarn.historyServer.allowTracking": "true"
    }
    driver_memory = "20G"
    driver_cores = "10"
    #
    cmd_list = ["spark-submit", "--master", "yarn", "--driver-memory", driver_memory, "--driver-cores", driver_cores]
    if conf_job_file:
        f = open(conf_job_file)
        LOGGER.info("opening configuration file " + conf_job_file + " to load configuration for the Spark-Submit job")
        for line in f:
            cmd_list.append("--conf")
            cmd_list.append(line)
    else:
        for k, v in conf_environment.items():
            cmd_list.append("--conf")
            cmd_list.append(k + "=" + v)
        cmd_list.append(path)

    # we get & analyze stdout on the fly to get the app id of the spark-submit
    p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, bufsize=1)
    with p.stdout:
        app_id = get_applicationid(iter(p.stdout.readline, b''))
    LOGGER.info("launching the job spark" + app_id + " with the following command \n" + " ".join(cmd_list))
    p.wait()  # wait for the subprocess to exit
    LOGGER.info("Spark job" + app_id + " " + get_yarn_status(app_id))
    LOGGER.debug("return the app_id of the spark job")
    return app_id


def get_cubes():
    """
    :return: return a dataframe that contains all information about all cubes and stores them in cube.csv
    """
    url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cubes"
    response = requests.get(url=url, headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
    df_cubes_list = list()
    for cube in json.loads(response.text.encode('utf-8')):
        cube_row = dict()
        cube_row["name"] = cube["name"]
        cube_row["model"] = cube["model"]
        cube_row["segments"] = [e["name"] for e in cube["segments"]]
        cube_row["project"] = cube["project"]
        cube_row["is_streaming"] = cube["is_streaming"]
        cube_row["status"] = cube["status"]
        df_cubes_list.append(cube_row)
    df = pd.DataFrame(df_cubes_list)
    df.to_csv("cubes.csv", index=False)
    LOGGER.info("storing the info about all cubes in ./cubes.csv")
    return df


def get_job_id(response_build_cube):
    response = json.loads(response_build_cube.text)
    return response[0]


def merge_all_reduce(segments_list, cube_name):
    """
    :param segments_list: a list containing segment ids
    :return: the id of the final merged segment
    """
    final_segment = segments_list[0]
    LOGGER.debug("final segment is " + final_segment)
    for segment in segments_list[1:]:
        LOGGER.debug(" merging " + str(final_segment) + " with segment " + str(segment))
        final_segment = merge(final_segment, segment, cube_name)
    return final_segment


def merge_all_divide_conquer(segments_list, cube_name):
    """

    :param segments_list: list of all segments to merge
    :param cube_name:  the id of the final merged segment
    :return:
    """
    chunks = segments_list
    while len(chunks) > 1:
        chunks = [chunks[x:x + 2] for x in range(0, len(chunks), 2)]
        temp = list()
        for ll in chunks:
            if len(ll) > 1:
                temp.append(merge(ll[0], ll[1], cube_name))
            else:
                temp.append(ll[0])
        chunks = temp


def merge(start_segment, end_segment, cube_name):
    """
    :param start_segment: string type, the id of the first segment
    :param end_segment: string type, the id of the second segment
    :return: a dictionary containing the response of the merge put request
    """
    if start_segment is None:
        LOGGER.debug("the start segment doesn't exist")
        return dict()
    elif end_segment is None:
        LOGGER.debug("the end segment doesn't exist")
        return dict()
    else:
        LOGGER.debug("start segment is " + start_segment)
        LOGGER.debug("end segment is " + end_segment)
        LOGGER.info("merging the two segments : " + start_segment + " and " + end_segment)
        start_new_segment = start_segment.split('_')[0]
        end_new_segment = end_segment.split('_')[1]
        url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cubes/" + cube_name + "/build"
        data = {"startTime": time_to_unix(start_new_segment), "endTime": time_to_unix(end_new_segment),
                "buildType": "MERGE"}
        response_merge = requests.put(url, data=json.dumps(data), headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
        LOGGER.info("writing the response of the merge request in " + LoggerProperties.LOG_FILE)
        with open(LoggerProperties.LOG_FILE, "a+") as f:
            f.write(response_merge.text.decode('utf-8'))
        if not response_merge.ok:
            LOGGER.error("Exception : response" + str(response_merge.status_code))
            LOGGER.error("check the log file " + LoggerProperties.LOG_FILE + " for more information")
            with open(LoggerProperties.LOG_FILE, "a+") as f:
                f.write(response_merge.text.decode('utf-8'))
            response_dict = json.loads(response_merge.text.encode('utf-8'))
            if "exception " in response_dict:
                # if there are gaps between the two segments
                if "gaps" in response_dict["exception"]:
                    data["forceMergeEmptySegment"] = "true"
                    LOGGER.warn("there are gaps between the two segments")
                    response_rebuild = requests.put(url, data=json.dumps(data), headers=KylinProperties.HEADERS,
                                                    auth=('admin', 'KYLIN'))
                elif "empty" in response_dict["exception"]:
                    LOGGER.warn("One of the segments is empty")
                    data["forceMergeEmptySegment"] = "true"
                    response_rebuild = requests.put(url, data=json.dumps(data), headers=KylinProperties.HEADERS,
                                                    auth=('admin', 'KYLIN'))
                else:
                    LOGGER.error(
                        "Merging the segments failed. Read the file " + LoggerProperties.LOG_FILE + " for more details")
                    with open(LoggerProperties.LOG_FILE, "a+") as f:
                        f.write(json.load(response_merge.text.encode('utf-8'))["exception"])
                    return response_dict
                LOGGER.info("updating the csv file segments.csv")
                df = pd.read_csv("segments.csv", encoding="utf-8")
                # delete the two old segments
                df = df[(df["segments_id"] != start_segment) & (df["segment_id"] != end_segment)]
                # adding the new merged segment
                df.append(pd.DataFrame([{
                    "cube_name": cube_name,
                    "start_date": start_new_segment,
                    "end_date": end_new_segment,
                    "segment_id": start_new_segment + "_" + end_new_segment
                }]))
                return json.loads(response_rebuild.text)
        else:
            LOGGER.info("Launch merging successful. Status of the merging : PENDING")
            response_check = check_job_completed(response_merge.text)
            if response_check == "SUCCESS":
                LOGGER.info("merge of the segments " + start_segment + " with " + end_segment + "successful")
                LOGGER.info("updating the csv file segments.csv")
                df = pd.read_csv("segments.csv", encoding="utf-8")
                # delete the two old segments
                df = df[(df["segments_id"] != start_segment) & (df["segment_id"] != end_segment)]
                # adding the new merged segment
                df.append(pd.DataFrame([{
                    "cube_name": cube_name,
                    "start_date": start_new_segment,
                    "end_date": end_new_segment,
                    "segment_id": start_new_segment + "_" + end_new_segment
                }]))
                return json.loads(response_merge.text)
            else:
                LOGGER.error(
                    "merge of the segments " + start_segment + " with " + end_segment + "failed. Check the file " + " for more details")
                with open(LoggerProperties.LOG_FILE, "a+") as f:
                    f.write("job " + response_check.encode('utf-8'))
                return json.loads(response_merge.text)


def launch_build(start_date, end_date, cube_name):
    """
    :param cube_name: the name of the cube we want to build
    :param end_date: integer timestamp in milliseconds
    :param start_date: integer timestamp
    :return: a dictionary containing the response of the build cube api put request
    """

    # information about the new segment created after building the cube
    segments_row = dict()

    url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cubes/" + cube_name + "/build"
    data = {"startTime": start_date, "endTime": end_date, "buildType": "BUILD"}
    LOGGER.debug("Launching the build of the cube. Start date :" + str(start_date) + " end date " + str(end_date))
    response_build = requests.put(url, data=json.dumps(data), headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
    if response_build.ok:
        LOGGER.info("launching the build of the cube. Status : PENDING")
        result_building = check_job_completed(response_build.text)
        with open("response_build.json", "a+") as f:
            f.write(response_build.text)
        if result_building == "SUCCESS":
            LOGGER.info("building the cube successful")
            segment_id = json.loads(response_build.text)["name"].split('-')[2].strip()
            segments_row["segment_id"] = segment_id
            segments_row["start_date"], segments_row["end_date"] = segment_id.split('_')
            segments_row["cube_name"] = cube_name
            df = pd.DataFrame([segments_row])
            import os.path
            if os.path.isfile("segments.csv"):
                with open('segments.csv', 'a') as f:
                    df.to_csv(f, header=False, index=False)
            else:
                with open('segments.csv', 'w') as f:
                    df.to_csv(f, index=False)
            return json.loads(response_build.text)
        else:
            with open("job_status", "w+") as f:
                f.write(result_building)
            LOGGER.error("building of the cube failed. read the log file " + LoggerProperties.LOG_FILE + " for more information")
            return dict()
    else:
        LOGGER.error(
            "error launching the build of the cube. Check the log file " + LoggerProperties.LOG_FILE + " for more information")
        LOGGER.debug(response_build.text)
        with open("build_response.json", "a+") as f:
            f.write(response_build.text)
        return json.loads(response_build.text)


def refresh(cube_name, segment_id):
    """
    :param cube_name:
    :param segment_id: ID of the segment of the cube
    :return:
    """
    start_date, end_date = [time_to_unix(t) for t in segment_id.split('_')]
    data = {"startTime": start_date, "endTime": end_date, "buildType": "REFRESH"}
    url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cubes/" + cube_name + "/build"
    response = requests.put(url, data=json.dumps(data), headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
    if response.ok:
        LOGGER.info("refreshing the cube " + cube_name + " with the following segment id " + segment_id)
        check_job_completed(response)
    else:
        LOGGER.error(
            "Failed to refresh cube" + cube_name + " with the following segment id " + segment_id + "\n check the log file " + LoggerProperties.LOG_FILE + " for more information")
        with open(LoggerProperties.LOG_FILE, "a+") as f:
            f.write(response.text)
        return json.loads(response.text)


def refresh_star(a_b):
    return refresh(*a_b)


def refresh_all_parallelized(list_cubes):
    """
    :param cube_list: a list of tuples containing each and its segment
        e.g. [("MODEL_MQB_DEF_DMI","20170101000000_20181101000000"),("MODEL_MQB_DEF_DPP","20170101000000_20181101000000")]
    :return: list of json response return by each refresh function
    """

    LOGGER.info("creating the file segments.csv which contains all segments")
    get_segments()
    df = pd.read_csv("segments.csv")
    # create a pool of threads
    # the number of threadds = number of CPUs by default
    pool = ProcessPool()
    results = pool.map(refresh_star, itertools.izip(df["cube name"], df["segment_id"]))
    pool.close()
    pool.join()
    return list(results)


def refresh_all_sequential():
    LOGGER.info("creating the file segments.csv which contains all segments")
    get_segments()
    df = pd.read_csv("segments.csv")
    updated_df = list()
    for row in df.iterrow():
        LOGGER.info("Launching the refresh of the segments of the cube " + row["cube_name"] + " ")
        response = refresh(row["cube_name"], row["segment_id"])
        if response.ok:
            updated_df.append(row)
    LOGGER.info("updating segments.csv with the existing segments")
    pd.DataFrame(updated_df).to_csv("segments.csv")


def get_htables(project_name):
    response = requests.get("http://localhost:7070/kylin/api/tables?project=" + project_name, auth=('admin', 'KYLIN'))
    result = list()
    for line in json.loads(response.text.encode('utf-8')):
        result.append(line["name"])
    return result


def get_yarn_status(app_id, port=8088):
    """
    :param app_id: get the log of a spark job
    :param port: port of yarn
    :return:
    """
    url = "http://" + KylinProperties.HOSTNAME + ":" + str(port) + "/ws/v1/cluster/apps/" + app_id
    response = requests.get(url)
    return str(json.loads(response.text.encode('utf-8'))["app"]["finalStatus"])


def get_lookups(model):
    url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/model/" + model
    response = requests.get(url=url, headers=KylinProperties.HEADERS, auth=('admin', "KYLIN"))
    lookup_tables = list()
    for lookup_table in json.loads(response.text.encode('utf-8'))["lookups"]:
        lookup_tables.append(lookup_table["table"])
    return lookup_tables, response


def get_model(cube_name):
    url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cube_desc/" + cube_name
    response = requests.get(url=url, headers=KylinProperties.HEADERS, auth=('admin', "KYLIN"))
    return json.loads(response.text.encode('utf-8'))[0]["model_name"]


def purge_cube(cube_name):
    LOGGER.info("disabling the cube " + cube_name)
    requests.post(url="http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cubes/" + cube_name + "/disable", headers=KylinProperties.HEADERS,
             auth=KylinProperties.AUTH)
    result = requests.post(url="http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cubes" + cube_name + "/purge",
                      auth=KylinProperties.AUTH, headers=KylinProperties.HEADERS)
    cmdList = ["sh", KylinProperties.KYLIN_DIRECTORY + "/bin/kylin.sh", "org.apache.kylin.tool.StorageCleanupJob", "--delete", "True"]
    subprocess.Popen(cmdList)
    return result


def earliest_date(list_segments):
    """
    :param list_segments: a list of segments e.g.
    ["20120101000000_20120601000000","20120601000000_20121201000000"]
    :return: the earliest segment
    """
    # create a list of tuples
    # The tuple contains (the date of the end date of a segment converted to timestamp, the segment)
    segments_by_end_date = [(time_to_unix(segment.split("_")[1]), segment) for segment in list_segments]
    segments_by_end_date.sort(key=lambda x: x[0])
    return segments_by_end_date[0][1]


def refresh_with_purge(cube_name, list_segments):
    """
    :param list_segments: an array containing the list of all segments of the cube
    :return: the final segment containing all the data of the cube
    """
    purge_cube(cube_name)
    LOGGER.debug("get the segment with the earliest date")
    earliest_segment = earliest_date(list_segments)
    end_date_timestamp = time.time() * 1000
    t = datetime.datetime.fromtimestamp(end_date_timestamp / 1000)
    end_segment = str(t.year) + str(t.month) + str(t.day) + str(t.hour) + str(t.minute) + str(t.day)
    response_build = launch_build(time_to_unix(earliest_segment.split('_')[0]), end_date_timestamp, cube_name)
    return earliest_segment.split('_')[0] + "_" + end_segment


def refresh_lookup_tables(segment_ids, cube_name):
    '''

    :param segment_ids: a list that contains the segments_ids to be refreshed.
    :return:
    '''
    LOGGER.info("Refreshing lookup tables for the cube " + cube_name + "with the segment IDs" + ",".join(segment_ids))
    responses = list()
    LOGGER.info("getting the model of the cube")
    model, response_get_model = get_model(cube_name, KylinProperties.HOSTNAME)
    if response_get_model.ok:
        LOGGER.info("getting the lookup tables")
        lookup_tables, response_get_lookup = get_lookups(model, KylinProperties.HOSTNAME)
        if response_get_lookup.ok:
            url = "htpp://" + KylinProperties.KylinProperties.HOSTNAME + ":7070/kylin/api/cubes/" + cube_name + "/refresh_lookup"
            # segment_id = latest_segment()
            for lookup_table in lookup_tables:
                data = {{"cubeName": cube_name,
                         "lookupTableName": lookup_table,
                         "segmentIDs": segment_ids}
                        }
                try:
                    response_refresh_lookup = requests.put(url, headers=KylinProperties.HEADERS, data=data, auth=KylinProperties.AUTH)
                    response_refresh_lookup.raise_for_status()
                except:
                    LOGGER.error(
                        'refreshing lookup table ' + lookup_table + ' failed. Check the log file ' + LoggerProperties.LOG_FILE + " for more information")
                    with open(LoggerProperties.LOG_FILE, "a+") as f:
                        f.write(response_get_lookup.text)
                responses.append(json.loads(response_refresh_lookup))
                LOGGER.info("refreshing table " + lookup_table)
            return responses
        else:
            LOGGER.error("Error could not load the lookup tables from the model of the cube")
            with open(LoggerProperties.LOG_FILE, "a+") as f:
                f.write(response_get_lookup.text)
                return json.loads(response_get_lookup)
    else:
        LOGGER.error("Failed to get the model of cube " + cube_name)
        with open(LoggerProperties.LOG_FILE, "a+") as f:
            f.write(response_get_model.text)
        return json.loads(response_get_model)


def get_jobs_df():
    """
    :return: create a dataframe containing all information about all jobs
    """
    # get all cubes
    jobs_list = json.loads(requests.get(url="http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/jobs"
                                    , headers=KylinProperties.HEADERS
                                    , auth=KylinProperties.AUTH).text.encode('utf-8'))
    df_list = list()
    for job in jobs_list:
        row_dict = dict()
        cube_name_deocnstructed = job["name"].split('-')
        row_dict["cube_name"] = cube_name_deocnstructed[1].stript()
        row_dict["type"] = job["type"]
        row_dict["segment_id"] = cube_name_deocnstructed[2].strip()
        row_dict["job_status"] = job["job_status"]
        row_dict["start_date"], row_dict["end_date"] = row_dict["segment_id"].split("_")
        df_list.append(row_dict())
    return pd.DataFrame(df_list)


def timestamp(t):
    return (t - datetime(1970, 1, 1)).total_seconds()


def time_to_unix(complete_date):
    '''

    :param complete_date: must be in this format YYYYmmDDhhmmss
    for eg : 20190101000000
    a date with the format YYYY/mm/dd/DD/hh/MM/ss works
    :return: unix timestamp in milliseconds


    if the date is after the local date, it is in local timezone, which is GTM+2 right here in Paris, Feb 8th, and not GMT which can be confusing
    not advised to put end date in the future because Kylin doesn't know how to handle them
    '''
    if "/" not in complete_date:
        year = complete_date[0:4]
        month, day, hour, minute, second = [complete_date[i:i + 2] for i in range(4, len(complete_date), 2)]
        datetime_time = datetime.strptime(year + month + day + hour + minute + second, "%Y%m%d%H%M%S")
    else:
        datetime_time = datetime.strptime(complete_date, "%Y/%m/%d/%H/%M/%S")
    return int(timestamp(datetime_time) * 1000)

class model(object):
    def __init__(self,name,own="ADMIN",):

        pass