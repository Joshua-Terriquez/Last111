import time
import sqlite3
from sqlite3 import Error

def return_list(filename):
with open(filename, "r") as file:
read_line = (file.read().splitlines())
return read_line


def openConnection(_dbFile):
print("++++++++++++++++++++++++++++++++++")
print("Open database: ", _dbFile)
conn = None
try:
conn = sqlite3.connect(_dbFile)
cursor = conn.cursor()
print("success")
except Error as e:
print(e)
print("++++++++++++++++++++++++++++++++++")
return conn


def closeConnection(_conn, _dbFile):
print("++++++++++++++++++++++++++++++++++")
print("Close database: ", _dbFile)
try:
_conn.close()
print("success")
except Error as e:
print(e)
print("++++++++++++++++++++++++++++++++++")



def build_data_cube(_conn):
print("++++++++++++++++++++++++++++++++++")
print("BUILD DATA CUBE")
total = (_conn.execute('select sum(price) from distributor;').fetchone())[0]
query = 'update price_cube set tot_price={} where dist_type="ALL" and
prod_type="ALL";'.format(total)
print(query)
#_conn.execute('update price_cube set tot_pr')
# max = randint(4,12)
print("++++++++++++++++++++++++++++++++++")




def print_Product(_conn):
print("++++++++++++++++++++++++++++++++++")
print("PRINT PRODUCT")
result = (_conn.execute('SELECT * FROM product;').fetchall())
l = '{:<20} {:<20} {:<20}'.format("model", "type", "maker")
print(l)
for item in result:
if item[2] is None:
print('{:<20} {:<20} {:<20}'.format(item[0],item[1],'NULL'))
else:
print('{:<20} {:<20} {:<20}'.format(item[0],item[1],item[2]))
print("++++++++++++++++++++++++++++++++++")




def print_Distributor(_conn):
print("++++++++++++++++++++++++++++++++++")
print("PRINT DISTRIBUTOR")
result = (_conn.execute('SELECT * FROM distributor;').fetchall())
l = '{:<20} {:<20} {:<20}'.format("model", "type", "maker")
print(l)
for item in result:
if item[2] is None:
print('{:<20} {:<20} {:<20}'.format(item[0],item[1],'NULL'))
else:
print('{:<20} {:<20} {:<20}'.format(item[0],item[1],item[2]))
print("++++++++++++++++++++++++++++++++++")



def print_Cube(_conn):
print("++++++++++++++++++++++++++++++++++")
print("PRINT DATA CUBE")
result = (_conn.execute('SELECT * FROM price_cube;').fetchall())
l = '{:<20} {:<20} {:>10} {:>10}'.format("dist", "prod", "cnt", "total")
print(l)
for item in result:
print('{:<20} {:<20} {:>10} {:>10}'.format(item[0], item[1],
item[2],item[3]))
print("++++++++++++++++++++++++++++++++++")



def modifications(_conn):
print("++++++++++++++++++++++++++++++++++")
print("MODIFICATIONS")
action_list = return_list('modifications.txt')
for action in action_list:
action = action.split(' ')
tableName = action[0]
if tableName.lower() == 'product' and action[1] == 'D':
query = 'delete from product where model="{}";'.format(action[2])
print("Running query: {}".format(query))
time.sleep(0.1)
_conn.execute(query)
elif tableName.lower() == 'product' and action[1] == 'I':
query = 'insert into product
values({},"{}","{}");'.format(action[2],action[3],action[4])
print("Running query: {}".format(query))
time.sleep(0.1)
_conn.execute(query)
elif tableName.lower() == 'distributor' and action[1] == 'D':
query = 'delete from distributor where model="{}" and
name="{}";'.format(action[2],action[3])
print("Running query: {}".format(query))
time.sleep(0.1)
_conn.execute(query)
elif tableName.lower() == 'distributor' and action[1] == 'I':
query = 'insert into distributor values({},"{}",
{});'.format(action[2],action[3],action[4])
print("Running query: {}".format(query))
time.sleep(0.2)
_conn.execute(query)
print("++++++++++++++++++++++++++++++++++")





def main():
database = r"data.sqlite"
# create a database connection
connect = openConnection(database)
with connect:
conn = connect.cursor()
print_Product(conn)
print_Distributor(conn)
build_data_cube(conn)
print_Cube(conn)
modifications(conn)
print_Product(conn)
print_Distributor(conn)
print_Cube(conn)
closeConnection(connect, database)
if __name__ == '__main__':
main()
