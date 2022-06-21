class Menu_Sql:
    def __init__(self):
        self.menu_options = {
            1: 'Show Database Tables',
            2: 'Insert',
            3: 'Read',
            4: 'Update',
            5: 'Delete',
            6: 'Exit'
        }
    def print_menu(self):
        for key in self.menu_options.keys():
            print(key, "--", self.menu_options[key])    
        
    def print_menu_insert(self):
        pass

