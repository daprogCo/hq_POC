from connect_db import connect_db

def list_ongoing(cursor):
    cursor.execute("""
        SELECT id
        FROM pannes
        WHERE fin IS NULL
    """)
    return cursor.fetchall()

def set_ended_panne(conn, cursor, panne_id, timestamp):
    cursor.execute("""
        UPDATE pannes
        SET fin = %s
        WHERE id = %s
    """, (timestamp, panne_id))
    conn.commit()

def get_latest_statuts(cursor, panne_id, timestamp):
    cursor.execute("""
        SELECT id
        FROM statuts
        WHERE id_panne = %s AND timestamp = %s
        LIMIT 1
    """, (panne_id, timestamp))
    return cursor.fetchone() is not None

def get_ended_pannes(conn, cursor, timestamp):
    ongoing = list_ongoing(cursor)
    ended = [panne for panne in ongoing if not get_latest_statuts(cursor, panne[0], timestamp)]
    for panne in ended:
        panne_id = panne[0]
        set_ended_panne(conn, cursor, panne_id, timestamp)
    return ended

def main(data):
    conn = connect_db()
    if not conn:
        return "Database connection failed."
    cursor = conn.cursor()
    ended = get_ended_pannes(conn, cursor, data)
    cursor.close()
    conn.close()
    return ended