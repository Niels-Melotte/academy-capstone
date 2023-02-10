FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir
USER 0

ADD https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.9.0-spark_3.1/spark-snowflake_2.12-2.9.0-spark_3.1.jar /opt/spark/jars/spark-snowflake_2.12-2.9.0-spark_3.1.jar
ADD https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.3/snowflake-jdbc-3.13.3.jar /opt/spark/jars/snowflake-jdbc-3.13.3.jar
RUN chmod a+r /opt/spark/jars/spark-snowflake_2.12-2.9.0-spark_3.1.jar /opt/spark/jars/snowflake-jdbc-3.13.3.jar

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt --no-cache-dir

COPY . .

RUN pip3 install .
ARG spark_uid=185
USER ${spark_uid}