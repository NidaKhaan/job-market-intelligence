import requests
from datetime import datetime
import time
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import Database

class RemoteOKScraper:
    def __init__(self):
        self.api_url = "https://remoteok.com/api"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        }
        self.db = Database()
        self.jobs_scraped = 0
    
    def scrape(self, max_jobs=50):
        """Scrape jobs from RemoteOK API"""
        print(f"🚀 Starting RemoteOK API scraper...")
        print(f"📊 Target: {max_jobs} jobs")
        
        try:
            # Be respectful - wait before requesting
            time.sleep(2)
            
            response = requests.get(self.api_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            jobs_data = response.json()
            
            # First item is metadata, skip it
            jobs = jobs_data[1:max_jobs+1]
            
            print(f"✅ Retrieved {len(jobs)} jobs from API")
            print(f"🔍 Processing jobs...\n")
            
            for idx, job in enumerate(jobs, 1):
                try:
                    print(f"  [{idx}] Processing: {job.get('position', 'N/A')[:50]}...")
                    
                    job_data = self.parse_job(job)
                    
                    if job_data:
                        self.save_job(job_data)
                        self.jobs_scraped += 1
                        print(f"      ✓ Saved: {job_data['title'][:50]} at {job_data['company']}")
                    
                    time.sleep(random.uniform(0.1, 0.3))
                    
                except Exception as e:
                    print(f"      ❌ Error: {e}")
                    continue
            
            print(f"\n🎉 Scraping complete! Total jobs saved: {self.jobs_scraped}")
            return self.jobs_scraped
            
        except Exception as e:
            print(f"❌ API request failed: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def parse_job(self, job):
        """Parse job data from API response"""
        try:
            # Extract fields from API
            job_id = job.get('id', '')
            if not job_id:
                return None
            
            title = job.get('position', 'N/A')
            company = job.get('company', 'N/A')
            location = job.get('location', 'Remote')
            
            # Salary parsing - handle None and strings
            salary_min = None
            salary_max = None
            
            try:
                if job.get('salary_min'):
                    salary_min = float(job.get('salary_min'))
            except (ValueError, TypeError):
                pass
            
            try:
                if job.get('salary_max'):
                    salary_max = float(job.get('salary_max'))
            except (ValueError, TypeError):
                pass
            
            # Description
            description = job.get('description', '')
            if description and len(description) > 5000:
                description = description[:5000]
            
            # Tags/skills as requirements
            tags = job.get('tags', [])
            if isinstance(tags, list):
                requirements = ', '.join([str(tag) for tag in tags])
            else:
                requirements = ''
            
            if len(requirements) > 1000:
                requirements = requirements[:1000]
            
            # URL
            url = job.get('url', f"https://remoteok.com/remote-jobs/{job_id}")
            
            # Date - handle various formats
            posted_date = datetime.now()
            try:
                epoch_time = job.get('date')
                if epoch_time:
                    # Try as integer timestamp
                    if isinstance(epoch_time, str):
                        epoch_time = float(epoch_time)
                    posted_date = datetime.fromtimestamp(epoch_time)
            except (ValueError, TypeError, OSError):
                # If date parsing fails, use current time
                posted_date = datetime.now()
            
            job_data = {
                'job_id': f"remoteok_{job_id}",
                'title': title,
                'company': company,
                'location': location,
                'salary_min': salary_min,
                'salary_max': salary_max,
                'description': description,
                'requirements': requirements,
                'url': url,
                'source': 'RemoteOK',
                'posted_date': posted_date
            }
            
            return job_data
            
        except Exception as e:
            print(f"        ❌ Parse error: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def save_job(self, job_data):
        """Save job to database"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO jobs (job_id, title, company, location, salary_min, salary_max, 
                                description, requirements, url, source, posted_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    company = EXCLUDED.company,
                    location = EXCLUDED.location,
                    salary_min = EXCLUDED.salary_min,
                    salary_max = EXCLUDED.salary_max,
                    description = EXCLUDED.description,
                    requirements = EXCLUDED.requirements,
                    scraped_at = CURRENT_TIMESTAMP
            """, (
                job_data['job_id'],
                job_data['title'],
                job_data['company'],
                job_data['location'],
                job_data['salary_min'],
                job_data['salary_max'],
                job_data['description'],
                job_data['requirements'],
                job_data['url'],
                job_data['source'],
                job_data['posted_date']
            ))
            
            conn.commit()
            
        except Exception as e:
            print(f"        ❌ Database save error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.db.return_connection(conn)

if __name__ == "__main__":
    scraper = RemoteOKScraper()
    scraper.scrape(max_jobs=50)