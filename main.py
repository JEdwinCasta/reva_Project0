import sys
from sql_connection import Connect_to_dbImp
from menu import Menu_Sql
import helper
from IPython.display import display
import pandas as pd

class Main(Menu_Sql):
    def __init__(self):
        super().__init__()

if __name__=='__main__':
    main = Main()
    connect = Connect_to_dbImp
    connection = connect.create_server_connection("localhost", "root", "Sophie&Emma1981", "Clinic_database")
    connect.insert_data_from_JsonFile("data.json", connection)

    #main menu goes here
    
    while(True):
        main.print_menu()
        option = ''
        try:
            option = int(input('Enter your choice: '))
        except:
            print("wrong input. Please enter a  number...")

        #Check what choice was entered and act accordingly
        #option 1 to show tables
        if option == 1:
            query = """ SHOW TABLES;"""
            results = connect.read_query(connection, query)
            #Initialise empty list
            from_db = []
            for result in results:
                from_db.append(result)
            columns = ["Tables in Clinic DB"]
            df = pd.DataFrame(from_db, columns=columns)
            display(df)
        #Option 2 insert data in the tables
        elif option == 2:
            str1 = ""
            option2 = str(input('Enter table name: '))
            list_res = helper.check_table_name(option2)
            connect.execute_list_query(connection, list_res[0], list_res[1])
        #Read
        elif option == 3:
            sql="""SELECT * FROM doctor"""
            results = connect.read_query(connection, sql)
            #Initialise empty list
            from_db = []
            for result in results:
                from_db.append(result)
            columns = ["Doctors Id", "Last name", "First Name", "Speciality", "cost"]
            df = pd.DataFrame(from_db, columns=columns)
            display(df)
        #Update
        elif option == 4:
            table_name = str(input('Enter table name: '))
            column_name = str(input("enter the column name:"))
            new_inf = str(input("Type new information: "))
            cond = int(input("Enter the id: "))

            sql = """UPDATE %s SET %s = '%s' WHERE docid = %d; """ %(table_name,column_name,new_inf,cond)         
            connect.execute_query(connection, sql)

        #Delete
        elif option == 5:
            table_name = str(input('Enter table name: '))
            column_name = str(input("enter the column name:"))
            cond = int(input("Enter the id: "))

            sql = """DELETE FROM %s WHERE %s = %d;""" %(table_name,column_name,cond)
            connect.execute_query(connection, sql)
        elif option == 6:
            exit()
        else:
            print('Invalid option. Please enter a number between 1 and 4.')

