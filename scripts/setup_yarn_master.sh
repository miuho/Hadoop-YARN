# this script should be in NFS shared folder

$HADOOP_PREFIX/bin/hdfs namenode -format <cluster_name>
$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
$HADOOP_YARN_HOME/sbin/yarn-daemon.sh start proxyserver --config $HADOOP_CONF_DIR
$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver --config $HADOOP_CONF_DIR

$HADOOP_HOME/sbin/yarn-daemon.sh  --config $HADOOP_CONF_DIR/amnode start nodemanager
