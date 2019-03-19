#!/usr/bin/env bash
#install python36 & git
sudo yum install -y python36
sudo yum install -y git
#install pandas
sudo python36 -m pip install pandas

#go to hadoop home directory
sudo su - hadoop
cd
#get package
git clone https://lucasdienis:254062e03892ec9322e36be5640ed749fb8c2df2@github.com/dktunited/KYLIN_USB

#Execute Brain.py
python36 /home/hadoop/KYLIN_USB/sources/run/brain.py

#taking off the termination protection
aws emr modify-cluster-attributes --cluster-id j-3KVTXXXXXX7UG --no-termination-protected

#Terminate cluster
aws emr terminate-clusters --cluster-ids j-3KVXXXXXXX7UG