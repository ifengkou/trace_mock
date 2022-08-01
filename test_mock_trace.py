# -*- encoding: utf8 -*-
'''
author: shenlongguang<https://github.com/ifengkou>
Date: 2021-09-12 11:14:20
'''
import sys
from mock_trace import MockTrace

if __name__ == "__main__":
    appid = 'trace001'
    day = '2022-07-02'
    save_path = './output/'
    if len(sys.argv) >= 3:
        appid = sys.argv[1]
        day = sys.argv[2]
        if len(sys.argv)==4:
            save_path = sys.argv[3]

def genenate_range(day,end_day):
    for i in range(day,end_day):
        print("{\"lower\":%s,\"upper\":%s}" % (i, i+1),end=",")

mock = MockTrace(appid=appid,day=day,save_path=save_path)
#mock.generate_schema()
mock.generate()
#mock.gen_mutil()

#genenate_range(20220701,20220730)

