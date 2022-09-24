<img width="273" src="https://maqroll.github.io/writings/2022/warning.jpg" alt="warning" />

This tool allows to clean old instances from a running Kafka Streams app. Useful if you set up [static assignment](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances) and prefer not to wait after major changes on your consumer group.

```
Option (* = required)               Description
---------------------               -----------
* --application-id <String: id>     The Kafka Streams application ID
                                      (application.id).
--bootstrap-servers <String: urls>  Comma-separated list of broker urls with
                                      format: HOST1:PORT1,HOST2:PORT2 (default:
                                      localhost:9092)
--config-file <String: file name>   Property file containing configs to be
                                      passed to admin clients and embedded
                                      consumer.
--execute <Boolean: execute>        Set this flag to true to execute remove
                                      command. Otherwise just will inform you
                                      of the result. (default: false)
--help                              Print usage information.
--instance-ids <String: list>       Comma-separated list of instance ids. The
                                      tool will remove this instances from the
                                      list of members of the consumer group.
--prefix-keep <String: prefix>      Remove all instances whose id doesn't match
                                      this prefix
--version                           Print version information and exit.
```

Unless you set `execute` parameter to true, it wouldn't do anything.

Both `instance-ids` and `prefix-keep` let you set up the instances to remove.

Running the tool without setting up a set of instances to remove will show you current instances.


