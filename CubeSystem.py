
# libraries to install
import requests
import json
import pandas as pd



import time
from os import listdir
from os.path import isfile, join

from KYLIN_USB.sources.cube_system.KylinClient import build_parallelized, refresh_with_purge_parallelized, \
    refresh_with_purge, launch_build, time_to_unix, latest_date, LOGGER
from KYLIN_USB.sources.cube_system.global_variables import KylinProperties
from KYLIN_USB.sources.tests.generate_model import HOSTNAME, HEADERS, AUTH


class cube_system(object):

    #TODO : the right relative path
    cubes_json_folder=r"..\sources\kylin\cubes"
    models_json_folder=r"..\sources\kylin\models"
    projet_json_folder=r"..\sources\kylin\projet"

    def __init__(self, day, project="", size_pool=5,tables=None):
        self.cubes = []
        self.day = day
        self.getCubesRest(project)
        self.project = project
        self.size_pool = size_pool
        if tables is None:
            self.tables=tables

    def execute(self):
        # data prep en amont
        self.load_all_projects()
        self.load_all_cubes()
        self.load_or_refresh_kylin_tables()
        self.load_all_cubes()


    #TODO : add status cube updated, empty, not empty but not updated
    def check_maj_cubes(self):
        self.get_all_segments()

    def initCubing(self):
        if self.day == 3:
            self.launchRefresh()
        else:
            self.launchIncrement()

    def getCubes(self):
        return self.cubes

    def getCubesRest(self, project=""):
        """

        :param project: if we want to restrict the list of cubes of one project
        :return: update of the the cube objects of our cubesystem
        """
        url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cubes"
        if project:
            url += "?projectName=" + project
        response_get_cube = requests.get(url=url, headers=KylinProperties.HEADERS, auth=KylinProperties.AUTH)
        df_segments = self.get_all_segments()["segment_id"]
        cubes_dict = {}
        for cube_dict in json.loads(response_get_cube):
            cube = Cube()
            cube.name = cube_dict["name"]
            # get all segments
            cubes_dict[cube.name] = cube
        for segment in df_segments.iterrows():
            c = cube_dict[segment["segment_id"]]
            c.segments.append(segment["segment_id"])
            self.cubes.append(c)


    def delete_model(self,model_name):
        url="http://"+HOSTNAME+":7070/kylin/api/models/"+model_name
        request_delete=requests.delete(url=url,headers=HEADERS,auth=AUTH)
        if not request_delete.ok:
            LOGGER.error(json.loads(request_delete)["msg"])
        else:
            LOGGER.info("deleting model "+model_name+" successful")



    def delete_cube(self,cube_name):
        url="http://"+HOSTNAME+":7070/kylin/api/models/"+cube_name
        request_delete=requests.delete(url=url,headers=HEADERS,auth=AUTH)
        if not request_delete.ok:
            LOGGER.error(json.loads(request_delete)["msg"])
        else:
            LOGGER.info("deleting model "+cube_name+" successful")

    def delete_project(self,project_name):
        url="http://"+HOSTNAME+":7070/kylin/kylin/api/projects/"+project_name
        request_delete=requests.delete(url=url,headers=HEADERS,auth=AUTH)
        if not request_delete.ok:
            LOGGER.error(json.loads(request_delete)["msg"])
        else:
            LOGGER.info("deleting model "+project_name+" successful")



    def get_current_cubes(self):
        url="http://"+HOSTNAME+":7070/kylin/api/cubes"
        request_get_cubes=requests.get(url=url,headers=HEADERS,auth=AUTH)
        return [Cube(cube["name"],cube["project"])
            for cube in json.loads(request_get_cubes)]

    def get_current_models(self):
        url="http://"+HOSTNAME+":7070/kylin/api/models"
        request_get_models=requests.get(url=url,headers=HEADERS,auth=AUTH)
        return [Model(model["name"],model["fact_table"],model["project"],model["partition_desc"]["partition_date_column"])
            for model in json.loads(request_get_models)]



    def drop_project(self,project):
        """
        drop eveything inside the project : models and cubes
        :param project:
        :return:
        """
        current_cubes=self.get_current_cubes()
        for cube in current_cubes:
            self.delete_cube(cube.name)
        for model in self.get_current_models():
            self.delete_model(model.name)
        self.delete_project(project)





    def get_all_segments(self):
        """
        :return: return a dataframe containing all information about all segments.
        the dataframe is stored in segments.csv
        """
        list_rows = list()
        url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/jobs"
        response = requests.get(url=url, headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
        for job in json.load(response.text):
            if job["job_status"] == "FINISHED":
                row = dict()
                job_complete_name = job["name"].split('-')
                row["cube_name"] = job["display_cube_nam"]
                segment_id = job_complete_name[2].strip()
                row["segment_id"] = segment_id
                row["start_date"] = segment_id.split('_')[0]
                row["end_date"] = segment_id.split('_')[1]
                row["operation"] = job["type"]
                row["start_time"] = job["steps"][0]["exec_start_time"]
                list_rows.append(row)
        df = pd.DataFrame(list_rows)
        df_build = df[df["operation"] == "BUILD"]
        df_merge = df[df["operation"] == "MERGE"]
        df_merge = df_merge.sort_values("start_time")
        for row_merge in df_merge.iterrow():
            # remove tje two old segments
            # assume we have 1_2 and 2_3 segments in df_build
            # and 1_3 in df_merge
            # we remove the segments 1_2 and 2_3
            df_build = df_build[(df_build["start_date"] != row_merge["start_date"]) and
                                (df_build["end_date"] != row_merge["end_date"])]
            # and add 1_3
            df_build.append(row_merge)
        df_build.to_csv("segments.csv", sep=";")
        return df_build

    def getCube(self, cube_name):
        # get the whole cube by searching its cube_name
        for cube in self.cubes:
            if cube.name == cube_name:
                return cube

    def launchIncrement(self):
        vte_cubes = [(cube.name, cube.segments) for cube in self.cubes if "VTE" in cube.name.upper()]
        non_vte_cubes = [cube for cube in self.cubes if "VTE" not in cube.name.upper()]

        for cube_seq in non_vte_cubes:
            cube_seq.build()
        build_parallelized(vte_cubes, self.size_pool)

    def launchRefresh(self):
        vte_cubes = [(cube.name, cube.segments) for cube in self.cubes if "VTE" in cube.name.upper()]
        non_vte_cubes = [cube for cube in self.cubes if "VTE" not in cube.name.upper()]
        for cube_seq in non_vte_cubes:
            cube_seq.refresh()
        refresh_with_purge_parallelized(vte_cubes, self.size_pool)

    def getRunningJobs(self):
        """
        :return: a list of jobs who are running. Each job is a dictionary.
        """
        url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/jobs"
        response = requests.get(url=url, headers=KylinProperties.HEADERS, auth=('admin', 'KYLIN'))
        return [job for job in json.load(response.text) if job["job_status"] == "RUNNING"]

    def create_model(self, json_path):
        with open(json_path, "r") as f:
            corrected = f.read().replace("\\", "").replace("\"{", "{").replace("}\"", "}")
        model_dict = json.loads(corrected)
        model_desc_data = model_dict["modelDescData"]
        partition_desc = model_desc_data["partition_desc"]
        obsolete_parameters = ["partition_time_column"]
        for column in obsolete_parameters:
            if column in partition_desc:
                del partition_desc[column]
        del model_desc_data["partition_desc"]

        model = Model(name_model=model_dict["modelName"], project=model_dict["project"], **model_desc_data,
                      **partition_desc)
        return model
    def edit_model(self, json_path_receive,new_model,
                    json_path_send=r"template_model_envelope.json"
                    , project="learn_kylin"):
        """

        :param json_path_send: this is a default file that contain the json body to send it in the post request
        :param json_path_receive: the json containing the model to edit
        :param project: the project of the model to edit
        :return: the json of the new model
        """
        with open(json_path_send, "r") as f:
            corrected = f.read().replace("\\", "").replace("\"{", "{").replace("}\"", "}")
        to_send = json.loads(corrected)
        f_receive = open(json_path_receive)
        to_receive = json.loads(f_receive.read())

        for k, v in to_send["modelDescData"].items():
            if k in to_receive:
                to_send["modelDescData"][k] = to_receive[k]
        to_send["project"] = project
        to_send["modelDescData"]["partition_desc"]["partition_condition_builder"] = "org.apache.kylin.metadata.model" \
                                                                  ".PartitionDesc$YearMonthDayPartitionConditionBuilder "

        to_send["modelDescData"]["name"]+="_"
        return to_send



    def dict_to_json(self, model_dict, output_file):
        """

        :param model_dict: the model in dictionary format
        :return: the dictionary in json format. The format can be parsed by kylin
        so it contains anti-slashes
        """
        desc_data = model_dict["modelDescData"]
        model_dict["modelDescData"] = json.dumps(desc_data)
        with open(output_file, "w") as f_2:
            f_2.write(json.dumps(model_dict))
        return json.dumps(model_dict)
    def json_to_dict(self,json_path):
        """

        :param json_path: the path of json file
        :return: a dictionary. Nested dictionaries as string in json file are also dictionaries
        """
        with open(json_path, "r") as f:
            corrected = f.read().replace("\\", "").replace("\"{", "{").replace("}\"", "}")
        return json.loads(corrected)


    def load_model(self, json_path):
        """
        :return: create a new model in our cubesysteù
        """
        url = "http://" + HOSTNAME + ":7070/kylin/api/models"
        result = requests.post(url=url, headers=HEADERS, auth=AUTH, data=json.loads(json_path))

    def load_edited_model(self, old_model_path):
        # TODO : eventual deletion of the model

        new_model_dict=self.edit_model(old_model_path)
        new_file=self.dict_to_json(new_model_dict, new_model_dict["modelDescData"]["name"] + "_updated.json")
        self.load_model(new_file)

    def load_cube(self, json_path):
        cube_dict=self.json_to_dict(json_path)
        cube_model=cube_dict["cubeDescData"]["model_name"]
        if not (self.is_model_exist(cube_model)):
            self.load_all_models_moins_fin()
        url = "http://" + HOSTNAME + ":7070/kylin/api/cubes"
        result = requests.post(url=url, headers=HEADERS, auth=AUTH, data=json.loads(json_path))

    def load_all_models_moins_fin(self):
        models_files_json = [f for f in listdir(self.models_json_folder) if isfile(join(self.models_json_folder, f))]
        for model_json in models_files_json:
            self.load_model(model_json)

    def load_project(self, json_path):
        url = "http://" + HOSTNAME + ":7070/kylin/api/projects"
        result = requests.post(url=url, headers=HEADERS, auth=AUTH, data=json.loads(json_path))

    def is_cube_exist(self,cube_name):
        url="http://"+HOSTNAME+":7070/kylin/api/cubes"+cube_name
        result_request=requests.get(url=url,headeers=HEADERS,auth=AUTH)
        return result_request.ok

    def is_model_exist(self,model_name):
        url="http://"+HOSTNAME+":7070/kylin/api/models"
        list_models=json.loads(
        result_request=requests.get(url=url,headeers=HEADERS,auth=AUTH)
        )
        for model in list_models:
            if model_name is model["name"]:
                return True
        return False

    def is_project_exist(self,project_name):
        url="http://"+HOSTNAME+":7070/kylin/api/projects"
        list_projects=json.loads(
        result_request=requests.get(url=url,headeers=HEADERS,auth=AUTH)
        )
        for model in list_projects:
            if project_name is model["name"]:
                return True
        return False

    def get_all_tables(self):
        cubes_json = [f for f in listdir(self.cubes_json) if isfile(join(self.cubes_json, f))]
        for cube_json in cubes_json:
            with open(cube_json) as f:
                cube_dict=json.loads(f.read())
                dimensions_list=cube_dict["cubeDescData"]["table"]
                self.tables=list(set([_["table"] for _ in dimensions_list]))
                return self.tables

    def load_or_refresh_kylin_tables(self,cardinality_json_path,project_name):
        url="http://"+HOSTNAME+":7070/kylin/api/tables/"
        tables=self.get_all_tables()
        with open(cardinality_json_path) as f:
            requests.post(url=url+",".join(tables)+",/"+project_name,data=json.loads(f.read()),headers=HEADERS,auth=AUTH)

    def load_all_projects(self):
        projects_files_json = [f for f in listdir(self.projet_json_folder) if isfile(join(self.projet_json_folder, f))]
        url = "http://" + HOSTNAME + ":7070/kylin/api/projects"

        for project_json in projects_files_json:
            with open(project_json) as f:
                requests.post(url=url,data=json.loads(f.read(),auth=AUTH,headers=HEADERS))

    def edit_model(self, json_path_receive,new_model,
                    json_path_send=r"template_model_envelope.json"
                    , project="learn_kylin"):
        """

        :param json_path_send: this is a default file that contain the json body to send it in the post request
        :param json_path_receive: the json containing the model to edit
        :param project: the project of the model to edit
        :return: the json of the new model
        """
        with open(json_path_send, "r") as f:
            corrected = f.read().replace("\\", "").replace("\"{", "{").replace("}\"", "}")
        to_send = json.loads(corrected)
        f_receive = open(json_path_receive)
        to_receive = json.loads(f_receive.read())

        for k, v in to_send["modelDescData"].items():
            if k in to_receive:
                to_send["modelDescData"][k] = to_receive[k]


        to_send["project"] = project
        to_send["modelDescData"]["partition_condition_builder"] = "org.apache.kylin.metadata.model" \
                                                                  ".PartitionDesc$YearMonthDayPartitionConditionBuilder"
        f_2=open("new_model.json","w")
        f_2.write(json.dumps(to_send))
        return json.dumps(to_send)

    def load_new_model(self, json_model):
        # TODO : eventual deletion of the model
        new_model=self.edit_model(json_model)
        self.load_model(new_model)

    def load_all_cubes(self):
        url="http://"+HOSTNAME+":7070/kylin/api/cubes"
        cubes_json = [f for f in listdir(self.cubes_json) if isfile(join(self.cubes_json, f))]
        for cube_json in cubes_json:
            with open(cube_json) as f:
                cube_dict=json.loads(f.read())
            cube_name=cube_dict["name"]
            result_request = requests.get(url=url+"/"+cube_name, auth=AUTH, headers=HEADERS)
            if not result_request.ok:
                self.load_cube(cube_json)


class Cube(object):
    def __init__(self, name="", project="", segments=None, is_streaming=False):
        self.project = project
        self.name = name
        self.is_streaming = is_streaming
        if segments is None:
            self.segments = []
        else:
            self.segments = segments

    def build(self):
        if not self.segments:
            return json.loads(launch_build(time_to_unix("20170101000000"), time.time() * 1000, self.name))
        else:
            return json.loads(launch_build(latest_date(self.segments), time.time() * 1000, self.name))

    def refresh(self):
        return json.loads(refresh_with_purge(self.name, self.segments)
                          )

    def get_model(self, cube_name):
        url = "http://" + KylinProperties.HOSTNAME + ":7070/kylin/api/cube_desc/" + cube_name
        response = requests.get(url=url, headers=KylinProperties.HEADERS, auth=KylinProperties.AUTH)
        return json.loads(response.text.encode('utf-8'))[0]["model_name"]

    def load_model_json(self, nmodel_name, cube_name, json_file):
        pass


class Model(object):
    # TODO : test if None is converted to null in json
    # TODO : what is hu_mo0_3 ? in the model json ?

    def __init__(self, name_model, fact_table, project
                 , partition_date_column, owner="ADMIN", lookups=None, dimensions=None,
                 metrics=None, filter_condition="", partition_date_start=0
                 , partition_date_format="yyyy-MM-dd",
                 partition_time_format="HH:mm:ss",
                 partition_condition_builder="org.apache.kylin.metadata.model"
                                             ".PartitionDesc$DefaultPartitionConditionBuilder",
                   description=""
                 ):
        self.description = description
        if metrics is None:
            self.metrics = []
        else:
            self.metrics = metrics
        if lookups is None:
            self.lookups = []
        else:
            self.lookups = lookups
        if dimensions is None:
            self.dimensions = []
        else:
            self.dimensions = dimensions
        self.partition_condition_builder = partition_condition_builder
        self.partition_time_format = partition_time_format
        self.partition_date_format = partition_date_format
        self.partition_date_start = partition_date_start
        self.filter_condition = filter_condition
        self.partition_date_column = partition_date_column
        self.project = project
        self.fact_table = fact_table
        self.owner = owner
        self.name_model = name_model

