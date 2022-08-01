## 环境准备
python3 -m venv venv

source venv/bin/activate 

pip install -r requirements.txt

## 配置
修改conf/base.ini

每天trace数量

```
trace_numb_per_day = 5000000
```

## 生成数据

nohup python test_mock_trace.py trace001 2022-07-07 > nohup.out 2>&1 &

## 导入数据

nohup java -jar kudu-mock-data-1.0-SNAPSHOT-jar-with-dependencies.jar ./trace_mock/output/trace001/trace_20220703.json trace001 kudu.properties >> nohup.out 2>&1 &

nohup java -CP kudu-mock-data-1.0-SNAPSHOT-jar-with-dependencies.jar  SampleCopy xxxx