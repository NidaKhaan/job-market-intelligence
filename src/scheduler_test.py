import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scheduler import JobScheduler

if __name__ == "__main__":
    print("🧪 TESTING SCHEDULER (5-minute intervals)")
    print("This will run scrapes every 5 minutes for testing")
    print("Press Ctrl+C to stop\n")
    
    scheduler = JobScheduler()
    
    # Test with 5-minute intervals (0.083 hours)
    scheduler.start(interval_hours=0.083)