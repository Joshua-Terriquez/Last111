import sqlite3
from sqlite3 import Error


def openConnection(_dbFile):
    print("++++++++++++++++++++++++++++++++++")
    print("Open database: ", _dbFile)

    conn = None
    try:
        conn = sqlite3.connect(_dbFile)
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

    print("++++++++++++++++++++++++++++++++++")


def print_Product(_conn):
    print("++++++++++++++++++++++++++++++++++")
    print("PRINT PRODUCT")

    l = '{:<20} {:<20} {:<20}'.format("model", "type", "maker")
    print(l)

    print("++++++++++++++++++++++++++++++++++")


def print_Distributor(_conn):
    print("++++++++++++++++++++++++++++++++++")
    print("PRINT DISTRIBUTOR")

    l = '{:<20} {:<20} {:>20}'.format("model", "name", "price")
    print(l)

    print("++++++++++++++++++++++++++++++++++")


def print_Cube(_conn):
    print("++++++++++++++++++++++++++++++++++")
    print("PRINT DATA CUBE")

    l = '{:<20} {:<20} {:>10} {:>10}'.format("dist", "prod", "cnt", "total")
    print(l)

    print("++++++++++++++++++++++++++++++++++")


def modifications(_conn):
    print("++++++++++++++++++++++++++++++++++")
    print("MODIFICATIONS")

    print("++++++++++++++++++++++++++++++++++")



def main():
    database = r"data.sqlite"

    # create a database connection
    conn = openConnection(database)
    with conn:
        print_Product(conn)
        print_Distributor(conn)

        build_data_cube(conn)
        print_Cube(conn)

        modifications(conn)

        print_Product(conn)
        print_Distributor(conn)
        print_Cube(conn)

    closeConnection(conn, database)


if __name__ == '__main__':
    main()
