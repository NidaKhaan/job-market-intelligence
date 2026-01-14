from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.remoteok_scraper import RemoteOKScraper
from scrapers.github_jobs_scraper import GitHubJobsScraper
from database import Database

class JobScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.db = Database()
        self.stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_run': None,
            'next_run': None
        }
    
    def scrape_all_sources(self):
        """Main scraping job - runs on schedule"""
        print("\n" + "=" * 60)
        print(f"🚀 SCHEDULED SCRAPE STARTED")
        print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        self.stats['total_runs'] += 1
        self.stats['last_run'] = datetime.now()
        
        try:
            total_scraped = 0
            
            # RemoteOK
            print("\n📡 Scraping RemoteOK...")
            remoteok = RemoteOKScraper()
            count1 = remoteok.scrape(max_jobs=30)
            total_scraped += count1
            
            # Remotive
            print("\n📡 Scraping Remotive...")
            remotive = GitHubJobsScraper()
            count2 = remotive.scrape(max_jobs=30)
            total_scraped += count2
            
            print("\n" + "=" * 60)
            print(f"✅ SCRAPE COMPLETE!")
            print(f"   • RemoteOK: {count1} jobs")
            print(f"   • Remotive: {count2} jobs")
            print(f"   • Total: {total_scraped} jobs")
            print("=" * 60)
            
            self.stats['successful_runs'] += 1
            self.log_run(success=True, jobs_scraped=total_scraped)
            
        except Exception as e:
            print(f"\n❌ SCRAPE FAILED: {e}")
            self.stats['failed_runs'] += 1
            self.log_run(success=False, error=str(e))
    
    def log_run(self, success=True, jobs_scraped=0, error=None):
        """Log scraping runs to database"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Create logs table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scraping_logs (
                    id SERIAL PRIMARY KEY,
                    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN,
                    jobs_scraped INTEGER,
                    error_message TEXT
                )
            """)
            
            cursor.execute("""
                INSERT INTO scraping_logs (success, jobs_scraped, error_message)
                VALUES (%s, %s, %s)
            """, (success, jobs_scraped, error))
            
            conn.commit()
            
        except Exception as e:
            print(f"⚠️  Logging error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.db.return_connection(conn)
    
    def start(self, interval_hours=1):
        """Start the scheduler"""
        print("=" * 60)
        print("🎯 JOB SCHEDULER INITIALIZED")
        print("=" * 60)
        print(f"⏱️  Scrape interval: Every {interval_hours} hour(s)")
        print(f"🔄 Press Ctrl+C to stop\n")
        
        # Add job to scheduler
        self.scheduler.add_job(
            func=self.scrape_all_sources,
            trigger=IntervalTrigger(hours=interval_hours),
            id='scrape_jobs',
            name='Scrape all job sources',
            replace_existing=True
        )
        
        # Run immediately on start
        print("🚀 Running initial scrape...")
        self.scrape_all_sources()
        
        # Start scheduler
        self.scheduler.start()
        
        # Keep running
        try:
            while True:
                # Show next run time
                next_run = self.scheduler.get_job('scrape_jobs').next_run_time
                self.stats['next_run'] = next_run
                
                print(f"\r⏳ Next scrape: {next_run.strftime('%Y-%m-%d %H:%M:%S')} | "
                      f"Runs: {self.stats['total_runs']} | "
                      f"Success: {self.stats['successful_runs']} | "
                      f"Failed: {self.stats['failed_runs']}", end='', flush=True)
                
                time.sleep(30)  # Update display every 30 seconds
                
        except (KeyboardInterrupt, SystemExit):
            print("\n\n🛑 Shutting down scheduler...")
            self.scheduler.shutdown()
            print("✅ Scheduler stopped")
    
    def get_logs(self, limit=10):
        """Get recent scraping logs"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT run_time, success, jobs_scraped, error_message
            FROM scraping_logs
            ORDER BY run_time DESC
            LIMIT %s
        """, (limit,))
        
        logs = cursor.fetchall()
        cursor.close()
        self.db.return_connection(conn)
        
        return logs

if __name__ == "__main__":
    scheduler = JobScheduler()
    
    # Start with 1 hour interval (change to test: 0.017 = ~1 minute)
    scheduler.start(interval_hours=1)