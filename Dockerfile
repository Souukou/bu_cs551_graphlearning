FROM nvidia/cuda:11.6.2-base-ubuntu18.04 
USER root
ENV DEBIAN_FRONTEND noninteractive
ENV M2_HOME "/apache-maven-3.8.8"
ENV FLINK_HOME "/flink-1.14.4"
ENV KAFKA_HOME "/kafka_2.12-0.11.0.0"
ENV PATH "${PATH}:${M2_HOME}/bin:${FLINK_HOME}/bin:${KAFKA_HOME}/bin:{$HOME}/.local/bin"
RUN apt-get update
RUN apt-get install -y wget curl less cmake
RUN apt-get install -y software-properties-common build-essential git
RUN add-apt-repository -y ppa:deadsnakes/ppa
RUN apt-get install -y python3.7 python3.7-dev python3.7-distutils
RUN curl https://bootstrap.pypa.io/get-pip.py | python3.7
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 0
RUN apt-get install -y librocksdb-dev libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev
RUN apt-get install -y openjdk-8-jdk unzip
RUN apt-get autoremove && apt-get clean
RUN pip3 install python-rocksdb
RUN pip3 install torch==1.13.1+cu116 --extra-index-url https://download.pytorch.org/whl/cu116 && pip3 cache purge
RUN pip3 install pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv torch_geometric -f https://data.pyg.org/whl/torch-1.13.0+cu116.html && pip3 cache purge
RUN pip3 install kafka-python && pip3 cache purge
RUN curl -sSL https://dlcdn.apache.org/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz | tar -xzv
RUN curl -sSL https://archive.apache.org/dist/flink/flink-1.14.4/flink-1.14.4-bin-scala_2.11.tgz | tar -xzv
RUN curl -sSL https://archive.apache.org/dist/kafka/0.11.0.0/kafka_2.12-0.11.0.0.tgz | tar -xzv
RUN curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v3.16.1/protoc-3.16.1-linux-x86_64.zip -o /tmp/protoc.zip && unzip /tmp/protoc.zip -d $HOME/.local && rm /tmp/protoc.zip
RUN sed -i.bak 's/taskmanager.numberOfTaskSlots: 1/taskmanager.numberOfTaskSlots: 2/' "${FLINK_HOME}/conf/flink-conf.yaml"
RUN git clone --recursive https://www.github.com/flink-extended/dl-on-flink /tmp/dl-on-flink && pip install /tmp/dl-on-flink/dl-on-flink-framework/python /tmp/dl-on-flink/dl-on-flink-pytorch/python && rm -rf /tmp/dl-on-flink
RUN apt-get install -y vim && sed -i "s/advertised.listeners=.*$/advertised.listeners=PLAINTEXT:\/\/rise.bu.edu:9092/" "${KAFKA_HOME}/config/server.properties"
RUN printf "#\041/bin/bash\nnohup ${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties &> /tmp/zookeeper.log & nohup ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties &> /tmp/server.log &" > $HOME/start_kafka.sh && chmod +x $HOME/start_kafka.sh
WORKDIR /opt
ENTRYPOINT ["/bin/bash", "--init-file", "/root/start_kafka.sh"]
