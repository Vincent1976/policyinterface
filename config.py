DIALECT = 'mysql+pymysql'
DRIVER = 'mysqldb'
USERNAME = 'root'
HOST = '120.133.142.144' #'124.70.184.203'
PORT = 15336 #3306
PASSWORD = 'sate1llite' #'7rus7U5!'
DATABASE = 'policydata'
SQLALCHEMY_DATABASE_URI = DIALECT + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(PORT) + '/' + DATABASE
SQLALCHEMY_TRACK_MODIFICATIONS = False