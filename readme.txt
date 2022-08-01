nohup python test_mock_trace.py trace001 2022-07-07 > nohup.out 2>&1 &

nohup java -jar kudu-mock-data-1.0-SNAPSHOT-jar-with-dependencies.jar /installer/trace_mock/output/trace001/trace_20220703.json trace001 kudu.properties >> nohup.out 2>&1 &