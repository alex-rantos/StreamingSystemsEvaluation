FROM openjdk:8
MAINTAINER Matt Forshaw (matthew.forshaw@ncl.ac.uk)
RUN apt-get update
RUN apt-get install -y --no-install-recommends apt-utils build-essential sudo git maven tree vim

# Create location for Flink savepoints.
RUN mkdir -p /path/to/savepoints \
	mkdir /flink \
	mkdir /rates 

# Installing Python3 and python dependencies
RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && apt-get install python3-dev \
  && pip3 install --upgrade pip
  
RUN pip3 install matplotlib \
  && pip3 install numpy \
  && pip3 install pandas \
  && pip3 install watchdog \
  && pip3 install wheel\
  && pip3 install jupyter
