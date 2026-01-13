from database import Database
from collections import Counter

db = Database()
conn = db.get_connection()
cursor = conn.cursor()

print("=" * 60)
print("📊 JOB MARKET DATABASE STATISTICS")
print("=" * 60)

# Total jobs
cursor.execute("SELECT COUNT(*) FROM jobs")
total = cursor.fetchone()[0]
print(f"\n📈 Total Jobs: {total}")

# By source
cursor.execute("SELECT source, COUNT(*) FROM jobs GROUP BY source ORDER BY COUNT(*) DESC")
print(f"\n🔹 By Source:")
for source, count in cursor.fetchall():
    print(f"   • {source}: {count} jobs")

# By location (top 10)
cursor.execute("""
    SELECT location, COUNT(*) as cnt 
    FROM jobs 
    GROUP BY location 
    ORDER BY cnt DESC 
    LIMIT 10
""")
print(f"\n🌍 Top 10 Locations:")
for location, count in cursor.fetchall():
    print(f"   • {location}: {count} jobs")

# Top companies
cursor.execute("""
    SELECT company, COUNT(*) as cnt 
    FROM jobs 
    GROUP BY company 
    ORDER BY cnt DESC 
    LIMIT 10
""")
print(f"\n🏢 Top 10 Companies Hiring:")
for company, count in cursor.fetchall():
    print(f"   • {company}: {count} jobs")

# Recent activity
cursor.execute("""
    SELECT DATE(scraped_at) as date, COUNT(*) as cnt 
    FROM jobs 
    GROUP BY DATE(scraped_at) 
    ORDER BY date DESC 
    LIMIT 7
""")
print(f"\n📅 Recent Scraping Activity:")
for date, count in cursor.fetchall():
    print(f"   • {date}: {count} jobs")

print("\n" + "=" * 60)

cursor.close()
db.return_connection(conn)