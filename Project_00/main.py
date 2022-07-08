import sys
sys.path
from termcolor import colored
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
    f = open("password.txt")
    passw= f.read()
    local="localhost"
    root="root" 
    db="Clinic_database"
    connection = connect.create_server_connection(local, root, passw, db)
    jf= str(input("Enter Json File to Insert Data into the Database: "))
    connect.insert_data_from_JsonFile(jf, connection)

    #main menu goes here
    while(True):
        print()
        print(colored("WELCOME TO MEDICAL CENTER".center(20, '*'), attrs=['bold']))
        main.print_menu()
        option = ''
        try:          
            option = int(input('Enter your choice: '))
        except:
            print("wrong input. Please enter a  number...")

        #Check what choice was entered and act accordingly
        # opt 1 Make appointments for new and existing patients
        if option == 1:
            opt_sub =''
            main.submenapp()
            try:          
                opt_sub = int(input('Enter your choice: '))
            except:
                print("wrong input. Please enter a  number...")
            # New patient           
            if opt_sub == 1:
                list_pat = helper.get_patient_attr()
                connect.new_pat_app(connection, list_pat[0], list_pat[1])
                #query for doctor
                dlname = str(input("Enter Doctor Last name: "))
                dfname = str(input("Enter Doctor First name: ")) 
                sql = """SELECT* FROM doctor WHERE dlname = '%s' and dfname = '%s' """ %(dlname,dfname)
                results = connect.read_query(connection, sql)
                columns = ["ID", "Doctor Last Name", "Doctor First Name", "Speciality", "Cost"]
                df = pd.DataFrame(results, columns=columns)
                display(df)

                list_cli_pat =helper.get_clinic_patient_attr()
                connect.new_pat_app(connection, list_cli_pat[0], list_cli_pat[1])
                list_vis= helper.get_visit_attr()
                connect.new_pat_app(connection, list_vis[0], list_vis[1])

                print("Appointment Successful!")
                print()
            #Existing Patient    
            elif opt_sub == 2:
                lname = str(input("Enter Last name: "))
                fname = str(input("Enter First name: "))
                sql = """SELECT* FROM patient WHERE planame = '%s' and pfname = '%s' """ %(lname,fname)
                results = connect.read_query(connection, sql)               
                columns = ["ID", "Last Name", "First Name", "Date of Birth"]
                df = pd.DataFrame(results, columns=columns)
                display(df)

                #query for doctor
                dlname = str(input("Enter Doctor Last name: "))
                dfname = str(input("Enter Doctor First name: ")) 
                sql = """SELECT * FROM doctor WHERE dlname = '%s' and dfname = '%s' """ %(dlname,dfname)
                results = connect.read_query(connection, sql)
                columns = ["ID", "Doctor Last Name", "Doctor First Name", "Speciality", "Cost"]
                df = pd.DataFrame(results, columns=columns)
                display(df)

                list_vis= helper.get_visit_attr()
                connect.new_pat_app(connection, list_vis[0], list_vis[1])

                print("Appointment Successful!")
                print()
            else:
                continue          
        #Doctor avaiilability for current week       
        elif option == 2:
            sql = """SELECT d.docid, d.dfname, d.dlname, d.speciality, vid 
            FROM doctor d 
            LEFT join visit v ON d.docid = v.docid 
            WHERE vid is null GROUP BY d.docid;"""
            results = connect.read_query(connection, sql)
            columns = ["ID", "Doctor Last Name", "Doctor First Name", "Speciality", "Visit ID"]
            df = pd.DataFrame(results, columns=columns)
            display(df)
        #Reschedule appointment
        elif option == 3:
            r = helper.resch_and_cancel(connect, connection)

            newvdate = str(input("New Visit Date('YYYY-MM-DD HH:MM:SS'): "))    

            updata_sql = """UPDATE visit 
                        SET vdate = '%s' 
                        WHERE pid= %d and docid = %d;""" %(newvdate, r[0], r[1])
            connect.update_data(connection, updata_sql)
        
        #Cancel Appointment
        elif option == 4:
            r = helper.resch_and_cancel(connect, connection)

            cancel_appoint_sql = """delete from visit 
                                    where pid= %d and docid = %d; """ %(r[0], r[1])
            connect.update_data(connection, cancel_appoint_sql)

        #Report
        elif option == 5:
           
            sql = """ SELECT pfname, dfname, vdate
                FROM patient p, doctor d, visit v
                 WHERE p.pid = v.pid and d.docid = v.docid;"""

            results = connect.read_query(connection, sql)
            columns = ["Patient First Name", "Doctor First Name", "Visit Date"]
            df = pd.DataFrame(results, columns=columns)
            display(df)
        elif option == 6:
            if connection.is_connected():
                connection.close()
            
            print("MySQL connection is closed")
            exit()
        else:
            print('Invalid option. Please enter a number between 1 and 4.')
   
