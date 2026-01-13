import sys
import os
from datetime import datetime
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.remoteok_scraper import RemoteOKScraper
from scrapers.github_jobs_scraper import GitHubJobsScraper
from database import Database

class MasterScraper:
    def __init__(self):
        self.db = Database()
        self.total_scraped = 0
        self.sources_stats = {}
    
    def run_all(self):
        """Run all scrapers"""
        print("=" * 60)
        print("🚀 MASTER SCRAPER - RUNNING ALL SOURCES")
        print("=" * 60)
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # Get initial count
        initial_count = self.get_job_count()
        print(f"📊 Current database: {initial_count} jobs\n")
        
        # Run RemoteOK
        print("🔹 Source 1: RemoteOK")
        print("-" * 60)
        remoteok = RemoteOKScraper()
        remoteok_count = remoteok.scrape(max_jobs=50)
        self.sources_stats['RemoteOK'] = remoteok_count
        print()
        
        # Run Remotive
        print("🔹 Source 2: Remotive")
        print("-" * 60)
        remotive = GitHubJobsScraper()
        remotive_count = remotive.scrape(max_jobs=50)
        self.sources_stats['Remotive'] = remotive_count
        print()
        
        # Final stats
        final_count = self.get_job_count()
        new_jobs = final_count - initial_count
        
        print("=" * 60)
        print("✅ SCRAPING COMPLETE!")
        print("=" * 60)
        print(f"📊 Statistics:")
        print(f"   • Initial jobs: {initial_count}")
        print(f"   • New jobs added: {new_jobs}")
        print(f"   • Total jobs now: {final_count}")
        print(f"\n📈 By Source:")
        for source, count in self.sources_stats.items():
            print(f"   • {source}: {count} jobs")
        print(f"\n⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        self.show_sample_jobs()
    
    def get_job_count(self):
        """Get total job count from database"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM jobs")
        count = cursor.fetchone()[0]
        cursor.close()
        self.db.return_connection(conn)
        return count
    
    def show_sample_jobs(self):
        """Show sample of latest jobs"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        print("\n🎯 Latest 10 Jobs:")
        print("-" * 60)
        
        cursor.execute("""
            SELECT title, company, location, source, scraped_at 
            FROM jobs 
            ORDER BY scraped_at DESC 
            LIMIT 10
        """)
        
        for idx, row in enumerate(cursor.fetchall(), 1):
            title, company, location, source, scraped_at = row
            print(f"{idx:2d}. {title[:40]:40s} | {company[:20]:20s} | {source}")
        
        cursor.close()
        self.db.return_connection(conn)

if __name__ == "__main__":
    master = MasterScraper()
    master.run_all()