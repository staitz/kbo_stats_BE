import sqlite3
import os

def create_indexes():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'kbo_stats.db')
    con = sqlite3.connect(db_path)
    
    commands = [
        "CREATE INDEX IF NOT EXISTS idx_hgl_player_id ON hitter_game_logs(player_id);",
        "CREATE INDEX IF NOT EXISTS idx_pgl_player_id ON pitcher_game_logs(player_id);",
        "CREATE INDEX IF NOT EXISTS idx_hst_player_id ON hitter_season_totals(player_id);",
        "CREATE INDEX IF NOT EXISTS idx_pst_player_id ON pitcher_season_totals(player_id);",
        "CREATE INDEX IF NOT EXISTS idx_hgl_date ON hitter_game_logs(game_date);",
        "CREATE INDEX IF NOT EXISTS idx_pgl_date ON pitcher_game_logs(game_date);"
    ]
    
    for cmd in commands:
        try:
            print(f"Executing: {cmd}")
            con.execute(cmd)
        except Exception as e:
            # Table might not exist yet, which is fine
            print(f"Error executing {cmd}: {e}")
            
    con.commit()
    con.close()
    print("Database indexes successfully created!")

if __name__ == "__main__":
    create_indexes()
