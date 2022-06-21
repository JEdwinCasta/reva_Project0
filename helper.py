from ctypes import addressof


def get_clinic_attr():
 
    address= str(input("Enter the address: ")) 
    postcode= int(input("Enter the postcode: "))
    phone= int (input("Enter phone number: "))
    cli_name= str(input("Enter the clinic name: "))

    sql = """INSERT INTO clinic (address, postcode, phone, cli_name) VALUES (%s, %s, %s, %s)"""

    list_cli=[sql, [(address, postcode, phone, cli_name)]]
    return list_cli

def get_doctor_attr():
    dlname= str(input("Enter last name: ")) 
    dfname= str(input("Enter first name: ")) 
    speciality= str(input("Enter doctor speciality: "))
    cost= float(input("Enter the cost: "))

    sql = """INSERT INTO doctor (dlname, dfname, speciality, cost) VALUES (%s, %s, %s, %s)"""

    list_doc=[sql, [(dlname, dfname, speciality, cost)]]
    return list_doc

def get_patient_attr():
    planame= str(input("Enter last name: ")) 
    pfname= str(input("Enter first name: ")) 
    bdate= str(input("Enter date of birth: "))
   

    sql = """INSERT INTO patient (planame, pfname, bdate) VALUES (%s, %s, %s)"""

    list_pat=[sql, [(planame, pfname, bdate)]]
    return list_pat

def get_visit_attr():
    vdate= str(input("Enter visit date: ")) 
    complaints= str(input("Enter complaint: ")) 
    status= str(input("Enter status: "))
    pid= int(input("Enter the patient id: "))
    DocId = int(input("Enter the doctor id: "))
   
    sql = """INSERT INTO visit (vdate, complaints, status, pid, DocId)  VALUES (%s, %s, %s, %s, %s);"""

    list_vis=[sql, [(vdate, complaints, status, pid, DocId)]]
    return list_vis

def get_prescription_attr():
    prmedicine = str(input("Enter medicine name: "))
    prusage = str(input("Enter prescription usage: "))
    vid = int(input("Enter the visit id: "))
    pid = int(input("Enter the patient id: "))

    sql = """INSERT INTO prescription (prmedicine, prusage, vid, pid) VALUES (%s, %s, %s, %s); """
    list_pre=[sql[(prmedicine, prusage, vid, pid)]]
    return list_pre   

def get_clinic_doctor_attr():

    DocId = int(input("Enter the doctor id: "))
    ClId =  int(input("Enter the clinic id: "))
    shift = str(input("Enter doctor shift: "))
    
    sql = """INSERT INTO clinic_doctor (DocId, ClId, shift)  VALUES (%s, %s, %s);"""
    list_doc_clic = [sql [(DocId, ClId, shift)]]
    return list_doc_clic

def get_clinic_patient_attr():
                
    pid = int(input("Enter the patient id: "))
    ClId = int(input("Enter the clinic id: "))
       
    sql = """INSERT INTO clinic_patient (pid, ClId) VALUES (%s, %s);"""
    list_cli_pat =[sql [(pid, ClId)]]
    return list_cli_pat


def check_table_name(table_name):
    if table_name == "clinic":
        result = get_clinic_attr()
        return result
    elif table_name == "doctor":
        result = get_doctor_attr()
        return result
    elif table_name=="patient":
        result = get_patient_attr()
        return result
    elif table_name == "visit":
        result = get_visit_attr()
        return result
    elif table_name == "prescription":
        result = get_prescription_attr()
        return result
    elif table_name == "clinic_doctor":
        result = get_clinic_doctor_attr()
        return result
    elif table_name == "clinic_patient":
        result = get_clinic_patient_attr()
        return result
    
def update_clinic():
    address: str
