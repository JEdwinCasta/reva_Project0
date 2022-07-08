class Menu_Sql:
    def __init__(self):
        self.menu_options = {
            1: 'Make Appointment',
            2: 'Doctor Availability',
            3: 'Reschedule an Appointment',
            4: 'Cancel an Appointment',
            5: 'Appointment Report',
            6: 'Exit'
        }
        self.submenapp_opt ={
            1: 'New Patient',
            2: 'Existing Patient',
            3: 'Return to Main Menu'
        }

    def print_menu(self):
        for key in self.menu_options.keys():
            print(key, "--", self.menu_options[key]) 

    def submenapp(self):
        for key in self.submenapp_opt.keys():
            print(key, "--", self.submenapp_opt[key])

