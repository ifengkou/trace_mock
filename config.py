# -*- encoding: utf8 -*-
import configparser
import json

config = configparser.ConfigParser()
config.read(filenames='./conf/base.ini')

print("==============全局配置信息===============")
for section in config.keys():
    print("[{s}]".format(s=section))
    for key in config[section]:
        print("{k} = {v}".format(k=key, v=config[section][key]))

global_cfg = config['GLOBAL']
rule_cfg = config['RULES']

print("---------------加载 属性 配置信息>>>>>>>>>>>>>")

properties_cfg ={}
with open("./conf/properties.json",'r') as p:
    properties_cfg = json.load(p)

if not 'event' in properties_cfg:
    raise KeyError("cannot find event's properties in conf/properties.json")

print("---------------加载 事件 配置信息>>>>>>>>>>>>>")
events_cfg =[]
with open("./conf/events.json",'r') as p:
    events_cfg = json.load(p)

if events_cfg == None or len(events_cfg) == 0:
    raise KeyError("cannot find events configuration in conf/events.json")

print("==============mock配置信息加载完成===============")