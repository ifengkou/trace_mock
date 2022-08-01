# -*- encoding: utf8 -*-
'''
fake  data
Date: 2021-09-05 18:49:27
'''
from cgi import parse_multipart
from datetime import datetime
from pdb import pm
from time import sleep
from mock_util import MockUtil
import os,json,random,hashlib
from config import global_cfg,rule_cfg,properties_cfg,events_cfg
import utils
from multiprocessing.pool import ThreadPool
import threading
from logger import Logger

class MockTrace:
    def __init__(self,appid='test001',day='2000-01-01',save_path='./') -> None:
        self.mock = MockUtil()
        self.file_lock = threading.Lock()
        self.log = Logger().get_log()
        self.allow_data_type = ('int', 'float', 'string','bool')
        self.appid=appid
        self.day=day
        self.save_path=save_path

        cfg_hour_weight = global_cfg['hour_weight_in_day'].split(',')
        self.hour_weight_in_day = list(map(lambda x: int(x), cfg_hour_weight))

        # 计算事件的起始时间戳(int)
        self.begin_unix_time= utils.to_unix_timestamp(self.day)
        self.end_unix_time = self.begin_unix_time + 86399*1000
        self.total_size= int(global_cfg.get('trace_numb_per_day'))

        self.event_base_properties = properties_cfg.get('event')

        ## 从事件配置中获取 code 和 weight
        self._events_weight_map = {x['code']:x['weight'] for x in events_cfg}
        self._events_id_map = {x['code']:x['id'] for x in events_cfg}
        self._events_map = {x['code']:x for x in events_cfg}
        ## 从全局规则配置中获取 $platform 字典，session 规则需要使用
        _platform_cfg = {}
        _pf_rules = str(rule_cfg['platform_rule']).split('|')
        _platform_cfg['elements'] = str(_pf_rules[0]).split(",")
        _platform_cfg['weight'] = list( int(x) for x in str(_pf_rules[1]).split(","))
        self._platform_cfg = _platform_cfg

        self.preset_properties= [{'key':'$day','data_type':'int'},
        {'key':'event_id','data_type':'int'},
        {'key':'user_id','data_type':'string'},
        {'key':'event_time','data_type':'long'},
        {'key':'$id','data_type':'int'},
        {'key':'event_name','data_type':'string'}]

    def generate_schema(self):
        pmaps = []
        preset_flag =[]
        for pmap in self.preset_properties:
            pmaps.append(pmap)
            preset_flag.append(pmap["key"])

        for pmap in self.event_base_properties:
            pmaps.append(pmap)
        for emap in events_cfg:
            ps = emap.get('properties')
            for pmap in ps:
                pmaps.append(pmap)
        
        cmap = self._genenate_columns(pmaps)
        columns = cmap["columns"]
        column_data_type = cmap["c_t_map"]
        print("CREATE TABLE kudu.default.xxxxx (")
        for c in columns:
            if c in preset_flag and c != 'event_name':
                print("%-20s\t%-20s WITH ( primary_key = true ) ,"%("\"%s\"" % c ,self._change_to_kudu_type(column_data_type[c])))
            else:
                print("%-20s\t%-20s WITH ( nullable = true ) ,"%("\"%s\"" % c ,self._change_to_kudu_type(column_data_type[c])))
        print(") \n ")


    @staticmethod
    def _genenate_columns(pmaps):
        columns = list()
        column_data_type = {}
        for pmap in pmaps:
            k = pmap.get('key')
            dt = pmap.get('data_type')
            if ',' in k:
                ks = k.split(',')
                dts = dt.split(',')
                for i in range(len(ks)):
                    if ks[i] not in columns:
                        column_data_type[ks[i]] = dts[i]
                        columns.append(ks[i])
            else:
                if k not in columns:
                    column_data_type[k] = dt
                    columns.append(pmap.get('key'))
        return {"columns":columns,"c_t_map": column_data_type}

    @staticmethod
    def _change_to_kudu_type(dt):
        if 'string' == dt:
            return 'varchar'
        if 'long' == dt:
            return 'bigint'
        if 'bool' == dt:
            return 'boolean'
        if 'float' == dt:
            return 'decimal(10,3)'
        return dt

    def gen_mutil(self):
        pool_size = 10
        avg_size = int(self.total_size / pool_size)
        remainder = int(self.total_size % pool_size)
        ls = []
        for i in range(0,pool_size):
            if i == pool_size -1:
                ls.append(avg_size + remainder)
            else:
                ls.append(avg_size)

        self.log.info("[START]===========")
        pool = ThreadPool(pool_size)
        pool.map(self.generate, ls)
        pool.close()
        pool.join()
        sleep(2)

    def generate(self,size=None):
        event_models = []
        _stat_events_cnt = 0
        if size is None:
            size = self.total_size

        self.log.info("[START]线程%s开始工作了，接收%d" % (threading.currentThread().ident, size))
        for i in range(0, size):
            base_properties = self.generate_properties(self.event_base_properties)
            which_hour = int(self.mock.choice_element((range(0,24)),self.hour_weight_in_day))
            _hour_range_begin = self.begin_unix_time + which_hour * 3600000
            session_time = random.randint(_hour_range_begin, _hour_range_begin + 3600000-1)
            _last_time = session_time
            event_time = _last_time + random.randint(1000, 600000)
            if event_time > self.end_unix_time: # 如果下一天记录 则返回。session首次启动事件肯定包含其中
                continue
            event_code = self.mock.choice_element(list(self._events_weight_map.keys()),list(self._events_weight_map.values()))
             # 获得当前event 配置
            _event=self._events_map.get(event_code)
            _event_properties = {}
            base_properties = self.generate_properties(self.event_base_properties)
            _event_properties.update(base_properties)
            if _event != None and _event.get('properties') != None:
                event_specific_properties = self.generate_properties(_event.get('properties'))
                _event_properties.update(event_specific_properties)
            # 组装事件数据
            _uid = self.mock.random_range_element(data_type="int",rule="10000~99999")
            event_model = self.assemble_trace_model(_uid,event_code,event_time,_event_properties)
            event_models.append(event_model)
            if len(event_models) >= 10000:
                self.log.info("线程%s 完成了1w条trace的生成" % threading.currentThread().ident)
                #dump_thread = threading.Thread(target=self._dump_file,args=(event_models,'trace'))
                #dump_thread.start()
                self._dump_file(batch_list=event_models,type='trace')
                _stat_events_cnt = _stat_events_cnt + len(event_models)
                event_models.clear()
        if len(event_models) > 0:
            self._dump_file(batch_list=event_models,type='trace')
            _stat_events_cnt = _stat_events_cnt + len(event_models)
        self.log.info(f"[END]线程{threading.currentThread().ident} 生成trace事件数:{_stat_events_cnt}")
        event_models = None

    def generate_properties(self, properties=[]):
        '''
        生成属性字典列表
        :param: properties - list<param_map>，map见generate_property()
        :return: dict. 合并后的属性字典
        '''
        dict_properties={}
        for pmap in properties:
            pdict = self.generate_property(pmap)
            dict_properties.update(pdict)
        return dict_properties

    def generate_property(self, param={'key': None, 'data_type': None, 'rules': None, 'weight': None}):
        '''
        生成属性字典（包括单属性 和 组合属性）
        :param: map     - 属性参数 {'key':'', 'data_type': '', 'rules': '', 'weight': ''} 
                        - key & data_type & rules 不能为空
        :return: dict. 属性字典（组合属性为合并后的）
        '''
        key = param.get('key')
        data_type = param.get('data_type')
        rules = param.get('rules')
        weight = param.get('weight')
        if not all([key, data_type, rules]):
            raise ValueError("single_property's key & data_type & rules cannot be None`")
        # 参数处理
        key = str(key)
        data_type = str(data_type)
        rules = str(rules)
        weight_list = []
        if weight:
            _weight_list = str(weight).split(',')
            for ws in _weight_list:
                try:
                    weight_list.append(float(ws))
                except:
                    raise ValueError(f"the weight `{ws}` contain a non-number`")
        if ',' in key:
            return self.combine_properties(key=key,data_type=data_type,rules=rules,weight=tuple(weight_list))
        else:
            return self.single_property(key=key,data_type=data_type,rules=rules,weight=tuple(weight_list))

    def combine_properties(self,key=None, data_type=None, rules=None, weight=()) -> dict:
        '''
        组合属性处理
        区别于单属性，组合属性是多个属性组成，各个属性之间的存在关联关系，取值只能从固定集合中取
        :param: key str 组合属性，用 `,` 分割
        :param: data_type str 组合属性的数据类型，用 `,` 分割
        :param: rules str 组合属性的取值集合，取值集合的不同取值用 `|` 分割，属性之间用`,`分割
        :param: weight tuple 取值集合元素的权重，数组tuple，长度要等同于rules分割后的个数
        :example: - key=商品id、商品名称、商品品类、商品价格、是否自营;
                  - data_type=int,string,string,float,bool;
                  - rules=1,可乐,饮料,2.5,1|2,方便面,食品,1.5,0
                  - weight=(3,5)

        :return: property_json {property:value}
        '''        
        if not all([key, data_type, rules,weight]):
            raise ValueError("combine_property's key & data_type & rules & weight cannot be None`")
        keys = str(key).split(",")
        data_types = str(data_type).split(",")
        combine_values = str(rules).split("|")
        if len(weight) != len(combine_values):
            raise ValueError(f"the key `{key}`:combine_property's the size of combine_values not equal the size of weight`")

        properties_numb = len(keys)
        if properties_numb != len(data_types):
            raise ValueError("combine_property's the size of keys not equal the size of data_types`")
        
        # 值处理
        elements = []
        map = {}
        for i,combine_value in enumerate(combine_values):
            values = str(combine_value).split(",")
            if properties_numb != len(values):
                raise ValueError("combine_property's the size of values not equal the size of keys`")
            key = str(hash(combine_value))
            map[key] = values
            elements.append(key)

        choice_hash_key = self.mock.choice_range_element(data_type='string',elements=(elements),weight=weight)
        choice_values = map.get(choice_hash_key)

        combine_properties = {}
        for i in range(0,len(keys)):
            combine_properties[keys[i]] = utils.convert_data_type(data_type=data_types[i],ele=choice_values[i])
        
        return combine_properties;

    def single_property(self,key=None, data_type=None, rules=None, weight=()) -> dict:
        '''
        单属性处理
        :param: key 属性code
        :param: data_type 属性的数据类型
        :param: rules 属性的取值规则或者取值集合
        :param: weight 取值集合元素的权重，rules 为取值集合时有效
        :example: key=省份
                  data_type=string
                  rules='北京,上海'
                  weight=(5,4)
        :example: key=省份
                  data_type=string
                  rules=@province@10
                  weight=None
        :example: key=age
                  data_type=int
                  rules=18~55
                  weight=None
        :example: key=age
                  data_type=int
                  rules=0~18,19~55,55~100
                  weight=(2,6,3)
        :example: key=reg_date
                  data_type=datatime
                  rules=2000-01-01~2020-01-01
                  weight=None

        :return: property_json {property:value}
        '''
        if data_type not in self.allow_data_type:
            raise ValueError(f"`{data_type} not support`")
        ele = None
        if weight == None or len(weight) == 0:
            # 无权重处理
            if '~' in rules:
                # 区间取值
                ele = self.mock.random_range_element(data_type=data_type, rule= rules)
            elif '@' in rules:
                # 特殊规则取值
                _rs = rules.split('@')
                if len(_rs) == 2:
                    # 随机值
                    ele = self.mock.random_element(rule=rules)
                if len(_rs) == 3:
                    # 随机 集合值
                    ele = self.mock.random_set_element(key=rules)
        else:
            # 有权重值 (0.4,0.6)，表示在给定的集合中按权重选择
            rule_array = rules.split(",")
            ele = self.mock.choice_range_element(data_type=data_type,elements=(rule_array),weight=weight)

        if ele == None:
            raise ValueError(f"`{key} configution is error`") 
        
        return {key:ele}
    
        
    def assemble_trace_model(self,uid,event_name,event_time,event_properties = {}):
        model_dict = {}
        #model_dict['appid'] = self.appid
        model_dict['$day'] = utils.mils_to_day(event_time)
        model_dict['user_id'] = str(uid)
        model_dict['event_id'] = self._events_id_map.get(event_name)
        model_dict['event_time'] = event_time
        
        _offset= random.randint(100000, 900000)
        _partition = random.randint(1, 4)
        model_dict['$id'] = _offset * 100 + _partition

        model_dict['event_name'] = event_name
        # 特殊属性
        model_dict.update(event_properties)
        return model_dict
    
    def _dump_file(self,batch_list=[],type='event'):
        if len(batch_list)>0:
            filedir = self.save_path+ self.appid+'/'
            if not os.path.exists(filedir):
                os.makedirs(filedir)
            
            filename = filedir + type+'_'+str(self.day).replace('-','')+'.json'
            self.file_lock.acquire()
            self.log.info(f'`{threading.currentThread().ident}`get lock ---------start dump')
            with open(filename,'a') as f:
                for one in batch_list:
                    #json_str = json.dumps(one , ensure_ascii=False)
                    #f.write(json_str)
                    json.dumps(one ,f , ensure_ascii=False)
                    f.write('\n')
                f.close()
            self.file_lock.release()
            self.log.info(f'`{threading.currentThread().ident}`release lock ---------end dump')
            batch_list = []