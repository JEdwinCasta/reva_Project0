import sys
from time import sleep
sys.path
import json
import mysql.connector
from mysql.connector import Error
from sql_connectAbs import Connect_to_db
import pandas as pd
from multipledispatch import dispatch


# creates server connection to mySql
class Connect_to_dbImp(Connect_to_db):
    
    def create_server_connection(host_name, user_name, user_password, db):
        connection = None
        try:
            connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db
            )
            print("MySQL Database connection successful")
            sleep(1)
        except Error as err:
            print(f"Error: '{err}'")

        return connection
    
    #insert data from command_line 
    def new_pat_app(connection, sql, val):
        cursor = connection.cursor()
        try:
            cursor.executemany(sql, val)
            connection.commit()
            print("Query successful")
            print()
            sleep(1)
        except Error as err:
            print(f"Error: '{err}'")

    # Read data
    def read_query(connection, query):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            print("returning query")
            print()
            sleep(1)
            return result
        except Error as err:
            print(f"Error: '{err}'")

    # Update data SQL queries
    def update_data(connection, query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
            print("Query successful")
            print()
            sleep(2)
        except Error as err:
            print(f"Error: '{err}'")


    # This function reads a Json file and inserts the data into the database
    def insert_data_from_JsonFile(jf, connection):
        file = open(jf)
        data = json.load(file)
        print("processing data...")
        sleep(1)
        for key, value in data.items():
            listTup = []
            lh: list = value
            if key == "clinic":
                for item in lh:
                    add = item.get("address")
                    pc = item.get("postcode")
                    ph = item.get("phone")
                    cli_name = item.get("cli_name")
                    listTup.append((add, pc, ph, cli_name))
                sql = """
                    INSERT INTO clinic (address, postcode, phone, cli_name)  
                    VALUES (%s, %s, %s, %s);
                """
                Connect_to_dbImp.new_pat_app(connection, sql, listTup)
            elif key == "doctor":
                for item in lh:
                    dlname = item.get("dlname")
                    dfname = item.get("dfname")
                    speciality = item.get("speciality")
                    cost = item.get("cost")
                    listTup.append((dlname, dfname, speciality, cost))
                sql = """
                    INSERT INTO doctor (dlname, dfname, speciality, cost)  
                    VALUES (%s, %s, %s, %s);
                """
                Connect_to_dbImp.new_pat_app(connection, sql, listTup)
            elif key == "patient":
                for item in lh:
                    planame = item.get("planame")
                    pfname = item.get("pfname")
                    bdate = item.get("bdate")
                    listTup.append((planame, pfname, bdate))
                sql = """
                    INSERT INTO patient (planame, pfname, bdate)  
                    VALUES (%s, %s, %s);
                """
                Connect_to_dbImp.new_pat_app(connection, sql, listTup)
            elif key == "visit":
                for item in lh:
                    vdate = item.get("vdate")
                    complaints = item.get("complaints")
                    status = item.get("status")
                    pid = item.get("pid")
                    DocId = item.get("DocId")
                    listTup.append((vdate, complaints, status, pid, DocId))
                sql = """
                    INSERT INTO visit (vdate, complaints, status, pid, DocId)  
                    VALUES (%s, %s, %s, %s, %s);
                """
                Connect_to_dbImp.new_pat_app(connection, sql, listTup)
            elif key == "prescription":
                for item in lh:
                    prmedicine = item.get("prmedicine")
                    prusage = item.get("prusage")
                    vid = item.get("vid")
                    pid = item.get("pid")
                    listTup.append((prmedicine, prusage, vid, pid))
                sql = """
                    INSERT INTO prescription (prmedicine, prusage, vid, pid)  
                    VALUES (%s, %s, %s, %s);
                """
                Connect_to_dbImp.new_pat_app(connection, sql, listTup) 
            elif key == "clinic_doctor":
                for item in lh:
                    DocId = item.get("DocId")
                    ClId = item.get("ClId")
                    shift = item.get("shift")
                    listTup.append((DocId, ClId, shift))
                sql = """
                    INSERT INTO clinic_doctor (DocId, ClId, shift)  
                    VALUES (%s, %s, %s);
                """
                Connect_to_dbImp.new_pat_app(connection, sql, listTup)
            elif key == "clinic_patient":
                for item in lh:
                    pid = item.get("pid")
                    ClId = item.get("ClId")
                    listTup.append((pid, ClId))
                sql = """
                    INSERT INTO clinic_patient (pid, ClId)  
                    VALUES (%s, %s);
                """
                Connect_to_dbImp.new_pat_app(connection, sql, listTup)

 
