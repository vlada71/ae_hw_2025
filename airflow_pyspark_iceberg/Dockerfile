FROM quay.io/astronomer/astro-runtime:12.4.0

# USER root
# COPY ./dbt_project ./dbt_project
# COPY --chown=astro:0 . .

# install dbt into a virtual environment
# copy the dbt project into this project if you want to expand on this project
# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --upgrade pip && \
#     pip install --no-cache-dir --default-timeout=100 -r dbt_project/dbt-requirements.txt  && \
#     cd dbt_project && dbt deps && cd .. && \
#     deactivate

ENV AWS_REGION=us-west-2

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;


# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME
