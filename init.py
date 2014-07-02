#!/usr/bin/python
import os
import yaml
conf = yaml.load(open("conf.yml"))
os.environ["ZKHOSTS"] = "|".join(conf["zkhosts"])

from chassis.database import init_db
init_db()
