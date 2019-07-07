# cloudera-homework
This repository is made as part of the interview process of Cloudera.

## How to use
Upload the content of bin/ to HDP sandbox somewhere (e.g. /tmp/homework). You can get the sandbox by either [downloading](https://www.cloudera.com/downloads/hortonworks-sandbox/hdp.html) it,
or if you don't have the necessary resources (like me :laughing:) deploying to a cloud provider, like [Azure](https://hortonworks.com/tutorial/sandbox-deployment-and-install-guide/section/4/).

Start HBase in Ambari, and enable Phoenix SQL in HBase configs.

In the bin/ folder there are several shell scripts. They should be used in the following order:
1. generate.sh
2. process.sh
3. any of the query_count_*.sh files

## Build
mvn package will create a fresh homework.jar under bin/
