import sqlite3
import json
import prettytable
import os

MIGRATION_FILE = os.path.dirname(__file__)+'/dbmigrations.json'

sqls = {'check_migrations_exists': "SELECT count(*) FROM sqlite_master WHERE type ='table' AND upper(name) = 'MIGRATION'",
        'find_last_migration': "SELECT coalesce(max(migration_id),0) FROM migration",
        'add_migration_entry': "INSERT INTO migration VALUES (?,?,?,current_timestamp)",
        'get_status': 'select status, count(*) cnt from tasks group by status',
        'add_task': 'INSERT INTO tasks (instance_id, group_id, name, tasktype, definition, parameters, createdate) values (?,?,?,?,?,?,current_timestamp)',
        'get_task': "select * from tasks where status='NEW' LIMIT 1",
        'lock_task': "update tasks set status='RUNNING', startdate=current_timestamp, resumedate=current_timestamp where id=?",
        'update_task': "update tasks set status=?, log=?, enddate=current_timestamp where id=?"
        }

def conn(db_filename, isolation_level='EXCLUSIVE', timeout=2):
    global sqls
    try:        
        conn = sqlite3.connect(db_filename) 
        conn.execute('BEGIN EXCLUSIVE')
    except sqlite3.Error as e:
        print(e)
        raise
    apply_migrations(conn)
    return conn


def apply_migrations(conn):
    global MIGRATION_FILE
    #check if there is migrations table
    cur = conn.cursor()    
    cur.execute(sqls['check_migrations_exists'])
    last_migration = cur.fetchone()[0]
    #find last applied migration script
    if last_migration > 0:
        cur.execute(sqls['find_last_migration'])
        last_migration = cur.fetchone()[0]
    with open(MIGRATION_FILE) as json_file:
        data = json.load(json_file)
    #apply migrations
    for m in data['migrations']:
        if int(m['id']) > last_migration:        
            conn.execute(m['sql'])
            conn.execute(sqls['add_migration_entry'], (m['id'],m['version'],m['sql'],))
            conn.commit()   

def get_status(db_filename):        
    c = conn(db_filename)
    c.row_factory = sqlite3.Row
    cur = c.cursor()    
    cur.execute(sqls['get_status'])
    t = prettytable.PrettyTable()
    t.field_names = ["Status", "Count"]
    for row in cur.fetchall():
        t.add_row([row['status'],row['cnt']])
    print('Tasks:')
    print(t)
    c.close()            
         
def add_task(db_filename, definition, instance_id=1, group_id=1, name="",tasktype="python", parameters=""):   
    c = conn(db_filename)
    try:
        c.execute(sqls['add_task'],(instance_id, group_id, name, tasktype, definition, parameters,))
        c.commit()
        print('Task %s added successfully.' % (task_name if ('task_name' in globals()) else '[no name]' ))
    except sqlite3.Error as e:        
        print('Error adding task: '+str(e))
    finally:        
        c.close()

def start_task(db_filename):   
    c = conn(db_filename)
    c.row_factory = sqlite3.Row
    try:
        cur = c.execute(sqls['get_task'])
        task = cur.fetchone()          
        if task:
            c.execute(sqls['lock_task'],(task['id'],))
            c.commit()
            print('Task with ID %s locked successfully.' % task['id'])
        return task                        
    except sqlite3.Error as e:        
        print('Error getting task: '+str(e))
    finally:        
        c.close()

def update_task(db_filename, task_id, status, log):   
    c = conn(db_filename)
    c.row_factory = sqlite3.Row
    try:
        cur = c.execute(sqls['update_task'],(status, str(log), task_id,))
        c.commit()
    except sqlite3.Error as e:        
        print('Error updating task: '+str(e))
    finally:        
        c.close()
