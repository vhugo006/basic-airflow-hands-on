## Postgresql Commands

```
# postgresql service

sudo service postgresql status
sudo service postgresql start
sudo service postgresql stop


# access postgres terminal
sudo su - postgres
    
    # interact with postgres
    psql
    
    
```

## Libraries

```
pip install psycopg2-binary

```

## Airflow commands
### Create a user

`airflow users create --role Admin --username username --email username@test.com --firstname Fist --lastname Lasr --password password`

### Stop the services

```
cat $AIRFLOW_HOME/airflow-scheduler.pid | xargs kill
cat $AIRFLOW_HOME/airflow-webserver.pid | xargs kill
```