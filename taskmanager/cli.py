import argparse
import sys
import db
from contextlib import redirect_stdout
import io
import time
import os
import subprocess
import shlex

DB_FILENAME = 'taskmanager.sqlite'

parser = argparse.ArgumentParser(description='PW taskmanager')

def print_exit(message):
    global parser
    print('Error: '+message)
    parser.print_help()
    sys.exit(1)

def cli():
    global parser
    global DB_FILENAME
    parser.add_argument('--db-file', dest='db_file', action='store', default=DB_FILENAME, help='Specify SQLite DB file to be used (default: {}'.format(DB_FILENAME))    
    
    manage = parser.add_argument_group('Manage Tasks')
    manage.add_argument('-a', "--add", dest='task_definition', action='store', help='Add task')    
    manage.add_argument('-Tn', "--task-name", dest='task_name', default="", action='store', help='')   
    manage.add_argument('-Tt', "--task-type", dest='task_type', default="python", action='store', help='')   
    manage.add_argument('-Tp', "--task-parameter", dest='task_parameter', default="", action='store', help='')   

    manage = parser.add_argument_group('Run Tasks')
    manage.add_argument('-r', "--run", dest='run_single', default=0, action='count', help='Run task')    
    manage.add_argument('-rL', "--run-loop", dest='run_loop', default=0, action='count', help='Run task in infinite loop') 
    args = parser.parse_args()        
    

    #Handle operations
    if args.task_definition:        
        db.add_task(db_filename = args.db_file, definition = args.task_definition, tasktype=args.task_type, parameters=args.task_parameter)
    elif args.run_single:
        run_task(args.db_file)
    elif args.run_loop:
        while True:
            run_task(args.db_file)
            time.sleep(0.01)
    db.get_status(args.db_file)    

def run_task(db_file):
    task = db.start_task(db_file)
    if task == None:
        print("No task to run.")
        return
    try:
        parameters = tuple(task['PARAMETERS'].split(','))
        command = task['DEFINITION'].format(*parameters)
    except:
        result = 'Error:', sys.exc_info()[1]
        status='ERROR'         
    if task['TASKTYPE'] == 'python':
        output = io.StringIO()
        with redirect_stdout(output):
            try:
                exec(command)
                result = output.getvalue()
                status='COMPLETED'
            except:
                result = 'Error:', sys.exc_info()[1]
                status='ERROR'
        db.update_task(db_file, task['id'], status, result)
    elif task['TASKTYPE'] == 'bash':
        output = io.StringIO()
        try:
            result = runcmd(command)
            status='COMPLETED'
        except:
            result = 'Error:', sys.exc_info()[1]
            status='ERROR'                
        db.update_task(db_file, task['id'], status, result)        


def runcmd(command):
    f = subprocess.Popen(shlex.split(command),stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)   
    f.wait()     
    output, errors = f.communicate()    
    return str(output)+'\\n'+str(errors)        