# Reads 2 streams of data, aggregates them and writes the combined logs to CLI

Start listening to the streams using:
```
python3 ./log_aggregator.py -a 8080 -b 9090
```

To test with a test data which writes to ports 8080 and 9090:
```
python3 ./test_stream.py
```
This sends randomized ordered data packets to the above ports.

To terminate stream aggregator, keyboard interrupt with "Ctrl + C".