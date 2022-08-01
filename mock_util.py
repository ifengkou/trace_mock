# -*- encoding: utf8 -*-
'''
mock 工具类， 实现以下mock函数：

- 按规则生成离散元素；例如 @email 表示随机生成一个邮箱
- 按规则生成固定数量元素，随机选取；例如 @email@10 表示随机生成10个邮箱，每次从中选取一个
- 数值区间 随机生成数值；例如 18～55 表示从 18到55 随机生成一个数字（int or float)
- 时间区间 随机生成时间；例如 2020-01-01～2020-12-31 表示从 2020-01-01～2020-12-31 随机生成一个日期
- 元素集合 按权重选择； 例如 元素集合 ('bj','sh')，权重 为 (5,4)，表示 bj占 5/9 sh占 4/9
- 区间值集合 按权重选择和生成区间值；例如 区间元素集合 ('0～18','19～55')，权重 为 (5,4)，表示 5/9的概率生成的数字在0-18之间 4/9的概率在19-55之间

支持以下规则
[ '@string', '@int', '@float', '@bool','@datetime',
  '@email', '@user_name', '@name', '@job', '@phone',
  '@ip', '@uri', '@url', '@mac', '@use_agent', '@uri_path',
  '@country', '@province', '@city', '@address',
  '@array_int','@array_string','@array_float' ]

author: shenlongguang<https://github.com/ifengkou>
Date: 2021-09-05 14:49:27
'''
from datetime import datetime,timedelta
import random
import re
import utils
from collections import OrderedDict
from faker import Faker
import time

class MockUtil:
    def __init__(self):
        self.faker = Faker('zh_CN')
        self.global_set = {}
        self.allow_rule = ['@string', '@int', '@float', '@bool','@datetime',
                           '@email', '@user_name', '@name', '@job', '@phone',
                           '@ip', '@uri', '@url', '@mac', '@use_agent', '@uri_path',
                           '@country', '@province', '@city', '@address',
                           '@array_int','@array_string','@array_float']
        self.single_range_pattern = re.compile(
            r'^(-?\d+(\.\d+)?|[1-9]\d{3}-\d{2}-\d{2}|[1-9]\d{3}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})~(-?\d+(\.\d+)?|[1-9]\d{3}-\d{2}-\d{2}|[1-9]\d{3}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})$')
        
        self.rule_pattern = re.compile(r'^(@[a-z|_]+)$')
        self.rule_set_pattern = re.compile(r'^(@[a-z|_]+)@([1-9]\d*)$')

    def random_element(self, rule="@string"):
        '''
        根据规则生成随机元素
        :param: rule str 生成规则 @开头的规则
        :return: element. 根据规则生成的随机元素.
        '''
        m = self.rule_pattern.match(rule)
        if not m:
            raise ValueError(f"`rule {rule} syntax error`")
        if rule not in self.allow_rule:
            raise ValueError(f"`rule {rule} syntax error, rule is not allowed`")
        if rule == '@string':  # 随机字符串
            return self.faker.pystr(min_chars=5, max_chars=12)
        if rule == '@int':  # 随机整数
            return self.faker.random_int()
        if rule == '@float':  # 随机浮点数
            return self.faker.pyfloat(min_value=1, max_value=100, right_digits=2,positive=True)
        if rule == '@bool':  # 随机整数
            return self.faker.boolean()
        if rule == '@datetime':  # 前三十天 随机时间
            return self.random_datetime()
        if rule == "@email":
            return self.faker.ascii_company_email()
        if rule == "@user_name":  # 用户名
            return self.faker.user_name()
        if rule == "@name":  # 人名
            return self.faker.name()
        if rule == "@job":
            return self.faker.job()
        if rule == "@phone":
            return self.faker.phone_number()
        if rule == "@ip":
            return self.faker.ipv4()
        if rule == "@uri":  # uri全地址 http://abc.com/login.html
            return self.faker.uri()
        if rule == "@url":  # url全地址 http://abc.com/
            return self.faker.url()
        if rule == "@mac":  # mac 地址
            return self.faker.mac_address()
        if rule == "@use_agent":
            return self.faker.user_agent()
        if rule == "@domain":  # www.abc.com
            return self.faker.domain_name()
        if rule == "@uri_path":  # admin/index
            return self.faker.uri_path()
        if rule == "@country":  # 国家
            return self.faker.country()
        if rule == "@province":
            return self.faker.province()
        if rule == "@city":
            return self.faker.city()
        if rule == "@address":  # 全地址
            return self.faker.address()
        if rule == "@array_int":  # int 数组
            list = []
            _t = random.randint(3,6)
            for i in range(0, _t):
                list.append(random.randint(10,100))
            return list
        if rule == "@array_string":  # 字符数组
            list = []
            _t = random.randint(3,6)
            for i in range(0, _t):
                list.append(self.faker.word())
            return list
        if rule == "@array_float":  # 浮点数组
            list = []
            _t = random.randint(3,6)
            for i in range(0, _t):
                list.append(self.faker.pyfloat(min_value=10, max_value=100, right_digits=2))
            return list

    def random_range_element(self, data_type="int", rule='0~100'):
        '''
        随机从 数值/时间区间中 随机获取一个元素
        :param: data_type str 元素数据类型，number/float/datetime
        :param: value 区间规则，最大值 和最小值 用 ~ 分割
        :return: element. 随机从 固定大小集合中 获取一个元素
        '''
        m = self.single_range_pattern.match(rule)
        if not m:
            raise ValueError(f"rule `{rule}` syntax error`")
        _ele = 0
        try:
            #min_str = m.group(1)
            #max_str = m.group(2)
            v_str = str(rule).split('~')
            min_str = v_str[0]
            max_str = v_str[1]
            if data_type == 'float':
                _ele = round(random.uniform(float(min_str), float(max_str)),2)
            elif data_type == 'int':
                _ele = self.faker.random_int(min=int(min_str), max=int(max_str), step=1)
            elif data_type == 'datetime':
                _ele = self.faker.date_time_between(
                    utils.to_date(min_str), utils.to_date(max_str))
        except:
            raise ValueError(f"rule `{rule}`, `float` range error`")
        return _ele

    def random_set_element(self, key='@string@10'):
        '''
        随机从 固定大小集合中 获取一个元素

        :param: key str 生成规则 @规则@固定集合大小
        :example: @string@10 表示 生成10个随机字符串，随机从中抽取一个返回
        :return: element. 随机从 固定大小集合中 获取一个元素
        '''
        # 校验key 是否符合规则
        m = self.rule_set_pattern.match(key)
        if not m:
            raise ValueError(f"`rule {key} syntax error`")
        rule = m.group(1)
        _numb = m.group(2)
        # 生成固定大小的集合
        elements = self._get_fixed_sizi_set(rule=rule,size = int(_numb))
        index = random.randint(0, len(elements)-1)
        return elements[index]

    def choice_element(self, elements=('a', 'b', 'c'), weight=(0.45, 0.30, 0.25)):
        '''
        从集合中根据权重值返回相应的元素；

        :param: elements set 可选元素集合. 
                - 复杂元素的处理，可以先将复杂元素生成hashkey，选取了key后再反向返回复杂元素值
        :param: weight set 中元素 选中的权重值
        :example: - elements = (a,b,c)，weight = (0.45,0.35,0.25) 
                    表示返回元素 约45%为a 约35%为b 约25%为c 
        :return: element. 随机从 固定大小集合中 获取一个元素
        '''
        if len(elements) != len(weight):
            raise ValueError("elements'size not equal weight's size")
        _elements = []
        for inx, ele in enumerate(elements):
            _elements.append((str(ele), weight[inx]))
        _ordered_dict = OrderedDict(_elements)
        return self.faker.random_element(_ordered_dict)

    def choice_range_element(self, data_type='int', elements=('0～18', '19～60', '60～100'), weight=(0.25, 0.5, 0.25)):
        '''
        从区间集合中根据权重值返回相应的元素；只支持 数值(int/float/datetime)区间
        
        :param: data_type 元素的数据类型
        :param: elements set 可选元素集合
        :param: weight set 中元素 选中的权重值
        :example: - 当data_type=number,elements=('0～18', '19～60', '60～100'),weight=(0.25, 0.5, 0.25)时，
                    返回的数值 25%落在0-18这个区间，50%落在19-60区间，25%落在60-100
        :return: element. 随机从 固定大小集合中 获取一个元素
        '''
        if len(elements) != len(weight):
            raise ValueError("elements'size not equal weight's size")
        # 按权重从区间选择随机数，只支持 int 和 float 以及 datatime
        # 不能提前初始化固定比例的集合，这样返回的值就固定了
        list = []
        for inx, ele in enumerate(elements):
            _numb = 0
            if '~' in str(ele):  # 如果是区间
                _numb = self.random_range_element(data_type=data_type, rule=ele)
            else:  # 如果不是区间，单个值直接转换数据类型；例如'0~18','19,'20~100'
                _numb = utils.convert_data_type(data_type=data_type,ele=ele)
            list.append((_numb, weight[inx]))
        _ordered_dict = OrderedDict(list)
        return self.faker.random_element(_ordered_dict)

    def random_datetime(self,before=None,after=None):
        stime = (datetime.now()+timedelta(days=-30))
        etime = datetime.now()
        if before:
            stime = utils.to_date(before)
        if after:
            etime = utils.to_date(after)
        ts = random.randint(int(time.mktime(stime.timetuple())),int(time.mktime(etime.timetuple())))
        return datetime.fromtimestamp(ts)


    def _get_fixed_sizi_set(self, rule='@string',size=10):
        '''
        按照规则获得固定大小的集合，集合为None 则生成一个
        '''
        key = self._get_global_set_key(rule,size)
        if key not in self.global_set:
            self._init_fixed_size_set(rule=rule, size=size)
        return self.global_set[key]

    def _init_fixed_size_set(self,  rule='@string',size=10):
        '''
        按照规则生成固定大小的集合
        '''
        values = []
        for i in range(0, size):
            values.append(self.random_element(rule=rule))
            self.global_set[self._get_global_set_key(rule,size)] = values

    def _get_global_set_key(self, rule, size):
        return rule + '@' + str(size)