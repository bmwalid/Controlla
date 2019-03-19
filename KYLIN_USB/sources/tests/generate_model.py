#!/usr/bin/python
import json
import unittest

# going to use threads instead of processes
# use them in parallel to launch API calls
# we have chosen threads over processes because they are performant with IO operations (API calls)
# import this lib
from os import listdir
from os.path import join, isfile

import requests

import KYLIN_USB.sources.cube_system.CubeSystem as cs

AUTH=("admin","KYLIN")
HEADERS = {'Content-type': 'application/json'}
HOSTNAME = "127.0.0.1"
class generate_model(unittest.TestCase):
    path=r"template_model_envelope.json"
    path_2=r"model_json.json"
    def test_open_folder(self):
        c=cs.CubeSystem(3)
        models_files_json = [f for f in listdir(c.models_json_folder) if isfile(join(c.models_json_folder, f))]




    def edit_model(self, json_path_receive,
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

    def correct_json_file(self,model_dict,output):
        """

        :param model_dict: a dictionary containing the data about the model
        :return:
        """

        desc_data = model_dict["modelDescData"]
        model_dict["modelDescData"] = json.dumps(desc_data)
        with open(output, "w") as f_2:
            f_2.write(json.dumps(model_dict))
        return output

    def load_model(self, json_path):
        """
        :return: create a new model in our cubesyste
        """
        with open(json_path, "r") as f:
            corrected = f.read().replace("\\", "").replace("\"{", "{").replace("}\"", "}")
        url = "http://" + HOSTNAME + ":7070/kylin/api/models"
        result = requests.post(url=url, headers=HEADERS, auth=AUTH, data=json.loads(corrected))
        print(result.text)


    def load_new_model(self, old_model_path):
        # TODO : eventual deletion of the model

        new_model_dict=self.edit_model(old_model_path)
        new_file=self.correct_json_file(new_model_dict, new_model_dict["modelDescData"]["name"] + "_updated.json")
        self.load_model(new_file)





if __name__ == '__main__':
    unittest.main()
