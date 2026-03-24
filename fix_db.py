import sqlite3

def fix_db():
    db_path = 'c:/Users/user/Desktop/Study/kbo_stat_project/kbo_stat_BE/kbo_stats.db'
    con = sqlite3.connect(db_path)
    
    tables_to_drop = [
        'auth_group', 'auth_group_permissions', 'auth_permission', 'auth_user', 
        'auth_user_groups', 'auth_user_user_permissions', 'django_admin_log', 
        'django_content_type', 'django_migrations', 'django_session',
        'api_errorreport'
    ]
    
    for table in tables_to_drop:
        print(f"Dropping {table}...")
        try:
            con.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception as e:
            print(f"Error dropping {table}: {e}")
            
    con.commit()
    con.close()
    print("Database cleaned up. Ready for migrate.")

if __name__ == "__main__":
    fix_db()
