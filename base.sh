mkdir airflow
cd airflow
pipenv --python 3.11
pipenv shell
export AIRFLOW_HOME=$(pwd)
pipenv install apache-airflow
pipenv install apache-airflow-providers-databricks
mkdir dags
airflow db init
airflow users create --username admin --firstname minhwa --lastname lee --role Admin --email minhwa.lee@test.com