FROM hailgenetics/hail:0.2.127

RUN apt-get update && \
    apt-get -y install \
    curl \
    unzip \
    python3 \
    python3-pip \
    openjdk-8-jre-headless \
    g++ \
    libopenblas-base \
    liblapack3 && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install hail

# Download GCS connector JAR file and place it in /usr/local/lib
RUN curl -LO https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.0.jar && \
    mv gcs-connector-hadoop3-2.2.0.jar /usr/local/lib/

# Configure Hadoop to use GCS connector by setting environment variables
ENV HADOOP_CLASSPATH=/usr/local/lib/gcs-connector-hadoop3-2.2.0.jar
ENV HADOOP_OPTIONAL_TOOLS=gcs
    
RUN curl -LO https://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20231211.zip && \
    unzip plink_linux_x86_64_20231211.zip && \
    mv plink /bin/ && \
    rm -rf plink_linux_x86_64_20231211.zip

WORKDIR /
