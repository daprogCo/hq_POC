FROM apache/airflow:2.9.0-python3.12
# Installer Java + dépendances pour Spark
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Définir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Installer Spark 3.5.0 (Hadoop 3)
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Créer répertoire Spark warehouse (utile pour certains jobs)
RUN mkdir -p /opt/airflow/spark-warehouse && chown -R airflow: /opt/airflow/spark-warehouse

USER airflow

# Installer les requirements Airflow (hors pyspark ici)
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

