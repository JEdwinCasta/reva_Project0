# abstract base class work   
from abc import ABC, abstractmethod   

class Connect_to_db(ABC):
    #create connection to server 
    @abstractmethod
    def create_server_connection(host_name, user_name, user_password):
        pass

    #create connection to server and connect to an existing database 
    @abstractmethod
    def create_server_connection(host_name, user_name, user_password, db):
        pass

    #create a database
    @abstractmethod
    def create_database(connection, query):
        pass
    
    # Create & Insert data SQL queries
    @abstractmethod
    def execute_query(connection, query):
        pass

    #insert data from command_line 
    @abstractmethod
    def execute_list_query(connection, sql, val):
        pass
    
    # Read data
    def read_query(connection, query):
        pass
   
    