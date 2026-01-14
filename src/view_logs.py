from scheduler import JobScheduler
from datetime import datetime

scheduler = JobScheduler()

print("=" * 60)
print("📋 SCRAPING LOGS")
print("=" * 60)

logs = scheduler.get_logs(limit=20)

if not logs:
    print("\nℹ️  No logs yet. Run the scheduler first!")
else:
    print(f"\n📊 Showing last {len(logs)} runs:\n")
    
    for i, (run_time, success, jobs_scraped, error) in enumerate(logs, 1):
        status = "✅ SUCCESS" if success else "❌ FAILED"
        
        print(f"{i:2d}. {status} | {run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"    Jobs scraped: {jobs_scraped}")
        
        if error:
            print(f"    Error: {error[:100]}")
        print()

print("=" * 60)