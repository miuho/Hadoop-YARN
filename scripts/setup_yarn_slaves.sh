# this script should be in NFS shared folder

export HADOOP_HOME=/opt/projects/advcc/hadoop/hadoop-2.2.0
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_PREFIX=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64

$HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode
$HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start nodemanager

