import json
def deconvert(json_path):
    with open(json_path,"r") as f:
        data=json.loads(f.read())
        desc_data=data["modelDescData"]
    data["modelDescData"]=json.dumps(desc_data)
    with open("corrected_model.json","w") as f_2:
        f_2.write(json.dumps(data))


#TODO : solve name duplicate conflict
deconvert(r"C:\Users\Wanis\PycharmProjects\KYLIN_USB\new_model.json")