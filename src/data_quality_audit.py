from database import Database
import re

db = Database()
conn = db.get_connection()
cursor = conn.cursor()

print("=" * 60)
print("🔍 DATA QUALITY AUDIT")
print("=" * 60)

# Check for missing critical fields
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN title IS NULL OR title = 'N/A' THEN 1 ELSE 0 END) as missing_title,
        SUM(CASE WHEN company IS NULL OR company = 'N/A' THEN 1 ELSE 0 END) as missing_company,
        SUM(CASE WHEN location IS NULL OR location = '' THEN 1 ELSE 0 END) as missing_location,
        SUM(CASE WHEN description IS NULL OR description = '' THEN 1 ELSE 0 END) as missing_description,
        SUM(CASE WHEN url IS NULL OR url = '' THEN 1 ELSE 0 END) as missing_url
    FROM jobs
""")

row = cursor.fetchone()
total = row[0]

print(f"\n📊 Missing Data Analysis (Total: {total} jobs):")
print(f"   • Missing Title: {row[1]} ({row[1]/total*100:.1f}%)")
print(f"   • Missing Company: {row[2]} ({row[2]/total*100:.1f}%)")
print(f"   • Missing Location: {row[3]} ({row[3]/total*100:.1f}%)")
print(f"   • Missing Description: {row[4]} ({row[4]/total*100:.1f}%)")
print(f"   • Missing URL: {row[5]} ({row[5]/total*100:.1f}%)")

# Check for potential duplicates (same title + company)
cursor.execute("""
    SELECT title, company, COUNT(*) as cnt
    FROM jobs
    GROUP BY title, company
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 10
""")

duplicates = cursor.fetchall()
print(f"\n⚠️  Potential Duplicates (same title + company):")
if duplicates:
    for title, company, count in duplicates:
        print(f"   • {title[:40]} at {company}: {count} entries")
else:
    print("   ✓ No duplicates found!")

# Check salary data quality
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN salary_min IS NOT NULL THEN 1 ELSE 0 END) as has_min,
        SUM(CASE WHEN salary_max IS NOT NULL THEN 1 ELSE 0 END) as has_max,
        SUM(CASE WHEN salary_min IS NOT NULL AND salary_max IS NOT NULL THEN 1 ELSE 0 END) as has_both
    FROM jobs
""")

row = cursor.fetchone()
print(f"\n💰 Salary Data:")
print(f"   • Total jobs: {row[0]}")
print(f"   • With min salary: {row[1]} ({row[1]/row[0]*100:.1f}%)")
print(f"   • With max salary: {row[2]} ({row[2]/row[0]*100:.1f}%)")
print(f"   • With both: {row[3]} ({row[3]/row[0]*100:.1f}%)")

# Sample some raw data
print(f"\n📝 Sample Raw Data (first 3 jobs):")
cursor.execute("SELECT title, company, requirements FROM jobs LIMIT 3")
for idx, (title, company, reqs) in enumerate(cursor.fetchall(), 1):
    print(f"\n   Job {idx}:")
    print(f"   Title: {title}")
    print(f"   Company: {company}")
    print(f"   Requirements: {reqs[:100]}..." if reqs and len(reqs) > 100 else f"   Requirements: {reqs}")

print("\n" + "=" * 60)

cursor.close()
db.return_connection(conn)