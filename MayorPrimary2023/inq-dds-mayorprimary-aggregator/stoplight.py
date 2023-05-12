from wrapper import wrapper

### ASK IF THERE'S FRESH DATA FROM KASTURI'S SCRIPT ###

def run_update_or_not(boolean_updated_variable):
    if boolean_updated_variable == True:
        wrapper()
        
boolean_updated_variable = True

run_update_or_not(boolean_updated_variable)