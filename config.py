DIALECT = 'mysql+pymysql'
DRIVER = 'mysqldb'
USERNAME = 'root'
HOST = '124.70.184.203'
PORT = 3306
PASSWORD = '7rus7U5!'
DATABASE = 'policydata'
SQLALCHEMY_DATABASE_URI = DIALECT + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(PORT) + '/' + DATABASE
SQLALCHEMY_TRACK_MODIFICATIONS = False