FROM nvidia/cuda:11.6.2-base-ubuntu18.04 
USER root
ENV DEBIAN_FRONTEND noninteractive
ENV M2_HOME "/apache-maven-3.8.8"
ENV FLINK_HOME "/flink-1.14.4"
ENV PATH "${PATH}:${M2_HOME}/bin:${FLINK_HOME}/bin"
RUN apt-get update

RUN apt-get install -y wget curl less cmake
RUN apt-get install -y software-properties-common build-essential
RUN add-apt-repository -y ppa:deadsnakes/ppa
RUN apt-get install -y python3.7 python3.7-dev python3.7-distutils
RUN curl https://bootstrap.pypa.io/get-pip.py | python3.7
RUN apt-get install -y librocksdb-dev libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev
RUN apt-get install -y openjdk-8-jdk
RUN apt-get autoremove && apt-get clean
RUN pip3 install python-rocksdb
RUN pip3 install torch==1.13.1+cu116 --extra-index-url https://download.pytorch.org/whl/cu116 && pip3 cache purge
RUN pip3 install pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv torch_geometric -f https://data.pyg.org/whl/torch-1.13.0+cu116.html && pip3 cache purge
RUN curl -sSL https://dlcdn.apache.org/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz | tar -xzv
RUN curl -ssL https://archive.apache.org/dist/flink/flink-1.14.4/flink-1.14.4-bin-scala_2.11.tgz | tar -xzv
RUN sed -i.bak 's/taskmanager.numberOfTaskSlots: 1/taskmanager.numberOfTaskSlots: 2/' "${FLINK_HOME}/conf/flink-conf.yaml"
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 0
RUN apt-get install -y git
RUN git clone --recursive https://github.com/flinkextended/dl-on-flink /tmp/dl-on-flink && pip install /tmp/dl-on-flink/dl-on-flink-framework/python /tmp/dl-on-flink/dl-on-flink-pytorch/python && rm -rf /tmp/dl-on-flink
WORKDIR /opt
CMD ["/bin/bash"]
