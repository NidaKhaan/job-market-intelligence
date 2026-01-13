from database import Database

db = Database()
conn = db.get_connection()
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM jobs")
count = cursor.fetchone()[0]

print(f"\n📊 Total jobs in database: {count}")

cursor.execute("""
    SELECT title, company, location, source 
    FROM jobs 
    LIMIT 10
""")

print("\n🎯 Sample jobs:")
for row in cursor.fetchall():
    print(f"  • {row[0]} at {row[1]} ({row[2]}) - Source: {row[3]}")

cursor.close()
db.return_connection(conn)