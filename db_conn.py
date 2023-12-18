import pymysql
from sqlalchemy import create_engine


conn=pymysql.connect(host='54.175.78.29',port=int(3306),user='fivreaduser',passwd='0Q4W3@pE^pb5Nu',db='dbFactorClickProd')


# Connect to the PostgreSQL database
dbschema_ds = 'datascience'
dbschema_public = 'public'

engine_public = create_engine(
    'postgresql://postgres:9QqnUdZvr6zWz4W@pg-aurora-serverless.cluster-cyzwrcs8gffc.us-east-1.rds.amazonaws.com/FIVANA_DB',
    connect_args={'options': '-csearch_path={}'.format(dbschema_public)}
)

engine_ds = create_engine(
    'postgresql://postgres:9QqnUdZvr6zWz4W@pg-aurora-serverless.cluster-cyzwrcs8gffc.us-east-1.rds.amazonaws.com/FIVANA_DB',
    connect_args={'options': '-csearch_path={}'.format(dbschema_ds)}
)