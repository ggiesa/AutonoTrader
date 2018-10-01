from ingestion import custom_data as cd, live
from utils.toolbox import DateConvert
from datetime import datetime



from utils.database import Database

sql = 'SHOW TABLES;'
tables = set(Database().execute(sql).iloc[:,0])
