#!/bin/bash

# abort script on command failure
set -e

# if the first argument is not equal to master or worker, then exit
if [ "$1" != "master" ] && [ "$1" != "worker" ]; then
    echo "Usage: ./setup.sh <master/worker> <ip>"
    exit 1
fi

ip="83.212.80.118"
# if the second argument exists, it is an ip
if [ "$2" != "" ]; then
    # if the second argument is not a valid ip (sort of), then exit
    if ! [[ $2 =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "Usage: ./setup.sh <master/worker> <ip>"
        exit 1
    fi
    ip=$2
fi

# set locale
echo 'Setting locale (/etc/default/locale)...'

echo 'LANGUAGE="en"' | sudo tee /etc/default/locale
echo 'LANG="en_US.UTF-8"' | sudo tee --append /etc/default/locale
echo 'LC_ALL="en_US.UTF-8"' | sudo tee --append /etc/default/locale

LANGUAGE="en"
LANG="en_US.UTF-8"
LC_ALL="en_US.UTF-8"

echo 'Setting locale... done'

# install dependencies
echo 'Installing dependencies...'

sudo apt-get --quiet update
sudo apt-get --fix-missing --quiet --yes install \
    git \
    sudo \
    curl \
    default-jdk \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    libxml2-dev \
    libxmlsec1-dev \
    libffi-dev \
    liblzma-dev

echo 'Installing dependencies... done'

# install and configure pyenv
echo 'Installing pyenv...'

curl --insecure https://pyenv.run | bash

echo '' >> $HOME/.bashrc
echo '# pyenv' >> $HOME/.bashrc
echo 'export PYENV_ROOT=$HOME/.pyenv' >> $HOME/.bashrc
echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"' >> $HOME/.bashrc
echo 'eval "$(pyenv init -)"' >> $HOME/.bashrc

export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

pyenv install '3.8.18'
pyenv global '3.8.18'

echo 'Installing pyenv... done'

# install python dependencies
echo 'Installing python dependencies...'

pip install --no-warn-script-location --upgrade pip
pip install --no-warn-script-location geopy pyspark

echo 'Installing python dependencies... done'

# install and configure hadoop/spark
echo 'Installing hadoop/spark...'

mkdir --parents $HOME/opt/bin
mkdir --parents $HOME/opt/data/hdfs
mkdir --parents $HOME/opt/data/name
mkdir --parents $HOME/opt/data/tmp

cd $HOME

curl --insecure --remote-name \
    https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

curl --insecure --remote-name \
    https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

tar --extract --gzip --directory $HOME/opt/bin --file $HOME/hadoop-3.3.6.tar.gz
tar --extract --gzip --directory $HOME/opt/bin --file $HOME/spark-3.5.0-bin-hadoop3.tgz

rm $HOME/hadoop-3.3.6.tar.gz
rm $HOME/spark-3.5.0-bin-hadoop3.tgz

ln --symbolic $HOME/opt/bin/hadoop-3.3.6 $HOME/opt/hadoop
ln --symbolic $HOME/opt/bin/spark-3.5.0-bin-hadoop3 $HOME/opt/spark

echo 'Installing hadoop/spark... done'

# configure hadoop/spark environment
echo 'Configuring hadoop/spark environment...'

echo '' >> $HOME/.bashrc
echo '# hadoop/spark' >> $HOME/.bashrc
echo 'export HADOOP_HOME="/home/user/opt/hadoop"' >> $HOME/.bashrc
echo 'export SPARK_HOME="/home/user/opt/spark"' >> $HOME/.bashrc
echo 'export HADOOP_INSTALL="$HADOOP_HOME"' >> $HOME/.bashrc
echo 'export HADOOP_MAPRED_HOME="$HADOOP_HOME"' >> $HOME/.bashrc
echo 'export HADOOP_COMMON_HOME="$HADOOP_HOME"' >> $HOME/.bashrc
echo 'export HADOOP_HDFS_HOME="$HADOOP_HOME"' >> $HOME/.bashrc
echo 'export HADOOP_YARN_HOME="$HADOOP_HOME"' >> $HOME/.bashrc
echo 'export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native"' >> $HOME/.bashrc
echo 'export PATH="$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$SPARK_HOME/bin;"' >> $HOME/.bashrc
echo 'export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"' >> $HOME/.bashrc
echo 'export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"' >> $HOME/.bashrc
echo 'export LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH"' >> $HOME/.bashrc
echo 'export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"' >> $HOME/.bashrc

export HADOOP_HOME="/home/user/opt/hadoop"
export SPARK_HOME="/home/user/opt/spark"
export HADOOP_INSTALL="$HADOOP_HOME"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export HADOOP_YARN_HOME="$HADOOP_HOME"
export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native"
export PATH="$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$SPARK_HOME/bin;"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH"

echo 'export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"' >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"

# configure hadoop core site
echo '<?xml version="1.0" encoding="UTF-8"?>' > $HADOOP_HOME/etc/hadoop/core-site.xml
echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '<!--' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  Licensed under the Apache License, Version 2.0 (the "License");' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  you may not use this file except in compliance with the License.' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  You may obtain a copy of the License at' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '    http://www.apache.org/licenses/LICENSE-2.0' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  Unless required by applicable law or agreed to in writing, software' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  distributed under the License is distributed on an "AS IS" BASIS,' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  See the License for the specific language governing permissions and' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  limitations under the License. See accompanying LICENSE file.' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '-->' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '<configuration>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '    <name>hadoop.tmp.dir</name>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '    <value>/home/user/opt/data/tmp</value>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '    <name>fs.defaultFS</name>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '    <value>hdfs://okeanos-master:54310</value>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/core-site.xml
echo '</configuration>' >> $HADOOP_HOME/etc/hadoop/core-site.xml

# configure hadoop hdfs site
echo '<?xml version="1.0" encoding="UTF-8"?>' > $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '<!--' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  Licensed under the Apache License, Version 2.0 (the "License");' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  you may not use this file except in compliance with the License.' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  You may obtain a copy of the License at' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '    http://www.apache.org/licenses/LICENSE-2.0' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  Unless required by applicable law or agreed to in writing, software' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  distributed under the License is distributed on an "AS IS" BASIS,' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  See the License for the specific language governing permissions and' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  limitations under the License. See accompanying LICENSE file.' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '-->' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '<configuration>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '    <name>dfs.datanode.data.dir</name>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '    <value>/home/user/opt/data/hdfs</value>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '    <name>dfs.namenode.name.dir</name>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '    <value>/home/user/opt/data/name</value>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '    <name>dfs.replication</name>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '    <value>1</value>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml
echo '</configuration>' >> $HADOOP_HOME/etc/hadoop/hdfs-site.xml

# configure hadoop yarn site
echo '<?xml version="1.0"?>' > $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '<!--' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  Licensed under the Apache License, Version 2.0 (the "License");' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  you may not use this file except in compliance with the License.' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  You may obtain a copy of the License at' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    http://www.apache.org/licenses/LICENSE-2.0' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  Unless required by applicable law or agreed to in writing, software' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  distributed under the License is distributed on an "AS IS" BASIS,' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  See the License for the specific language governing permissions and' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  limitations under the License. See accompanying LICENSE file.' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '-->' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '<configuration>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.resourcemanager.hostname</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>okeanos-master</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.resourcemanager.webapp.address</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo -n '    <value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo -n $ip >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo ':8088</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.nodemanager.aux-services</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>mapreduce_shuffle,spark_shuffle</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>org.apache.hadoop.mapred.ShuffleHandler</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>org.apache.spark.network.yarn.YarnShuffleService</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.nodemanager.aux-services.spark_shuffle.classpath</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>/home/user/opt/spark/yarn/*</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.nodemanager.resource.memory-mb</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>6144</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.scheduler.maximum-allocation-mb</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>6144</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.scheduler.minimum-allocation-mb</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>128</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  <property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <name>yarn.nodemanager.vmem-check-enabled</name>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '    <value>false</value>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '  </property>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml
echo '</configuration>' >> $HADOOP_HOME/etc/hadoop/yarn-site.xml

# configure hadoop workers
echo 'okeanos-master' > $HADOOP_HOME/etc/hadoop/workers
echo 'okeanos-worker' >> $HADOOP_HOME/etc/hadoop/workers

# configure spark environment
echo 'spark.eventLog.enabled          true' > $SPARK_HOME/conf/spark-defaults.conf
echo 'spark.eventLog.dir              hdfs://okeanos-master:54310/spark.eventLog' >> $SPARK_HOME/conf/spark-defaults.conf
echo 'spark.history.fs.logDirectory   hdfs://okeanos-master:54310/spark.eventLog' >> $SPARK_HOME/conf/spark-defaults.conf
echo 'spark.master                    yarn' >> $SPARK_HOME/conf/spark-defaults.conf
echo 'spark.submit.deployMode         client' >> $SPARK_HOME/conf/spark-defaults.conf
echo 'spark.driver.memory             1g' >> $SPARK_HOME/conf/spark-defaults.conf
echo 'spark.executor.memory           1g' >> $SPARK_HOME/conf/spark-defaults.conf
echo 'spark.executor.cores            1' >> $SPARK_HOME/conf/spark-defaults.conf

echo '' >> $SPARK_HOME/conf/spark-env.sh
echo 'export PYSPARK_PYTHON="/home/user/.pyenv/versions/3.8.18/bin/python3"' >> $SPARK_HOME/conf/spark-env.sh
echo 'export PYSPARK_DRIVER_PYTHON="/home/user/.pyenv/versions/3.8.18/bin/python3"' >> $SPARK_HOME/conf/spark-env.sh

echo 'Configuring hadoop/spark environment... done'

# configure hosts and hostname
echo 'Configuring hosts and hostname...'

echo '' | sudo tee --append /etc/hosts
echo '192.168.0.1	okeanos-master' | sudo tee --append /etc/hosts
echo '192.168.0.2	okeanos-worker' | sudo tee --append /etc/hosts

sudo hostnamectl set-hostname okeanos-$1

echo 'Configuring hosts and hostname... done'

# reboot
echo 'Rebooting in 10 seconds (Ctrl+C to cancel)...'

sleep 10

sudo reboot now
