# Hadoop Pseudo-Distributed Mode Setup Guide

## Overview

This document summarizes the Hadoop 3.4.1 installation and configuration in pseudo-distributed mode on Ubuntu 24.04 with Java 21.

## System Information

| Component | Version/Details |
|-----------|-----------------|
| OS | Ubuntu 24.04.3 LTS |
| Java | OpenJDK 21.0.9 |
| Hadoop | 3.4.1 |
| Mode | Pseudo-Distributed |

## Installation Directory

```
HADOOP_HOME=/home/anhth/hadoop
```

## Running Daemons

| Daemon | Description | Default Port |
|--------|-------------|--------------|
| NameNode | HDFS metadata management | 9100 (RPC), 9870 (Web UI) |
| DataNode | HDFS data storage | 9866 |
| SecondaryNameNode | HDFS checkpoint management | 9868 |
| ResourceManager | YARN resource scheduling | 8032 (RPC), 8088 (Web UI) |
| NodeManager | YARN container management | 8042 (Web UI) |

## Web Interfaces

| Service | URL |
|---------|-----|
| NameNode Web UI | http://localhost:9870 |
| ResourceManager Web UI | http://localhost:8088 |
| NodeManager Web UI | http://localhost:8042 |

## Configuration Files

All configuration files are located in `$HADOOP_HOME/etc/hadoop/`:

### core-site.xml
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9100</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/home/anhth/hadoop/tmp</value>
    </property>
</configuration>
```

### hdfs-site.xml
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///home/anhth/hadoop/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///home/anhth/hadoop/hdfs/datanode</value>
    </property>
</configuration>
```

### mapred-site.xml
```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
```

### yarn-site.xml
```xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
```

## Environment Variables

Added to `~/.bashrc`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
```

## Java 21 Compatibility

Added to `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` for Java 21 compatibility:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Additional settings for Java 21 compatibility
export HADOOP_OPTS="$HADOOP_OPTS --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED"
export YARN_RESOURCEMANAGER_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"
export YARN_NODEMANAGER_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"
```

## Common Commands

### Cluster Management

| Command | Description |
|---------|-------------|
| `start-all.sh` | Start all Hadoop daemons (HDFS + YARN) |
| `stop-all.sh` | Stop all Hadoop daemons |
| `start-dfs.sh` | Start HDFS daemons only |
| `stop-dfs.sh` | Stop HDFS daemons only |
| `start-yarn.sh` | Start YARN daemons only |
| `stop-yarn.sh` | Stop YARN daemons only |

### HDFS Commands

```bash
# List files
hdfs dfs -ls /

# Create directory
hdfs dfs -mkdir -p /user/anhth

# Upload file to HDFS
hdfs dfs -put localfile.txt /user/anhth/

# Download file from HDFS
hdfs dfs -get /user/anhth/file.txt ./

# View file content
hdfs dfs -cat /user/anhth/file.txt

# Delete file
hdfs dfs -rm /user/anhth/file.txt

# Delete directory
hdfs dfs -rm -r /user/anhth/directory

# Check HDFS status
hdfs dfsadmin -report
```

### YARN Commands

```bash
# List nodes
yarn node -list

# List applications
yarn application -list

# Kill application
yarn application -kill <application_id>
```

### MapReduce

```bash
# Run example Pi calculation
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar pi 2 5

# Run WordCount example
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar wordcount /input /output
```

## Directory Structure

```
/home/anhth/hadoop/
├── bin/                    # Hadoop binaries
├── sbin/                   # Start/stop scripts
├── etc/hadoop/             # Configuration files
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   ├── yarn-site.xml
│   └── hadoop-env.sh
├── hdfs/
│   ├── namenode/           # NameNode data
│   └── datanode/           # DataNode data
├── tmp/                    # Temporary files
├── logs/                   # Log files
└── share/hadoop/           # Hadoop libraries and examples
```

## Troubleshooting

### Check if daemons are running
```bash
ps aux | grep java | grep -E "NameNode|DataNode|ResourceManager|NodeManager" | grep -v grep
```

### View logs
```bash
# NameNode log
cat $HADOOP_HOME/logs/hadoop-*-namenode-*.log

# DataNode log
cat $HADOOP_HOME/logs/hadoop-*-datanode-*.log

# ResourceManager log
cat $HADOOP_HOME/logs/hadoop-*-resourcemanager-*.log
```

### Re-format NameNode (WARNING: Deletes all HDFS data)
```bash
stop-all.sh
rm -rf $HADOOP_HOME/hdfs/namenode/*
rm -rf $HADOOP_HOME/hdfs/datanode/*
rm -rf $HADOOP_HOME/tmp/*
hdfs namenode -format
start-all.sh
```

### Port conflicts
If default ports are in use, modify the port numbers in:
- `core-site.xml` - fs.defaultFS (HDFS port)
- `hdfs-site.xml` - dfs.namenode.http-address (Web UI port)

## Notes

1. **Port 9100**: The default HDFS port (9000) was changed to 9100 due to a port conflict on this system.

2. **Java 21**: Hadoop 3.4.1 requires additional JVM options for Java 21 compatibility due to module access restrictions.

3. **SSH**: Passwordless SSH to localhost is required and has been configured.

4. **Replication**: Set to 1 for pseudo-distributed mode (single node).

## References

- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/stable/)
- [HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [YARN Architecture](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)

---
*Setup completed: November 29, 2025*
