#!/usr/bin/python
import itertools
import json
import logging
import subprocess
import threading

import requests
import pandas as pd

# going to use threads instead of processes
# use them in parallel to launch API calls
# we have chosen threads over processes because they are performant with IO operations (API calls)
# import this lib
from multiprocessing.dummy import Pool as ThreadPool

HEADERS = {'Content-type': 'application/json'}
HOSTNAME = "127.0.0.1"

