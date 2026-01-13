import requests
from datetime import datetime
import time
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import Database

class GitHubJobsScraper:
    def __init__(self):
        # Using Remotive.io API (similar to GitHub Jobs which shut down)
        self.api_url = "https://remotive.com/api/remote-jobs"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        self.db = Database()
        self.jobs_scraped = 0
    
    def scrape(self, max_jobs=50):
        """Scrape jobs from Remotive API"""
        print(f"🚀 Starting Remotive scraper...")
        print(f"📊 Target: {max_jobs} jobs")
        
        try:
            time.sleep(2)
            
            response = requests.get(self.api_url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            jobs = data.get('jobs', [])[:max_jobs]
            
            print(f"✅ Retrieved {len(jobs)} jobs from API")
            print(f"🔍 Processing jobs...\n")
            
            for idx, job in enumerate(jobs, 1):
                try:
                    print(f"  [{idx}] Processing: {job.get('title', 'N/A')[:50]}...")
                    
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
            return 0
    
    def parse_job(self, job):
        """Parse job data from API response"""
        try:
            job_id = job.get('id', '')
            if not job_id:
                return None
            
            title = job.get('title', 'N/A')
            company = job.get('company_name', 'N/A')
            location = job.get('candidate_required_location', 'Remote')
            
            # Salary
            salary_text = job.get('salary', '')
            salary_min = None
            salary_max = None
            
            # Description
            description = job.get('description', '')
            if description and len(description) > 5000:
                description = description[:5000]
            
            # Category as requirements
            category = job.get('category', '')
            tags = job.get('tags', [])
            if isinstance(tags, list):
                requirements = f"{category}, " + ', '.join([str(tag) for tag in tags])
            else:
                requirements = category
            
            if len(requirements) > 1000:
                requirements = requirements[:1000]
            
            # URL
            url = job.get('url', '')
            
            # Date
            posted_date = datetime.now()
            try:
                date_str = job.get('publication_date', '')
                if date_str:
                    posted_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                pass
            
            job_data = {
                'job_id': f"remotive_{job_id}",
                'title': title,
                'company': company,
                'location': location,
                'salary_min': salary_min,
                'salary_max': salary_max,
                'description': description,
                'requirements': requirements,
                'url': url,
                'source': 'Remotive',
                'posted_date': posted_date
            }
            
            return job_data
            
        except Exception as e:
            print(f"        ❌ Parse error: {e}")
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
            print(f"        ❌ Database error: {e}")
            conn.rollback()
        finally:
            cursor.close()
            self.db.return_connection(conn)

if __name__ == "__main__":
    scraper = GitHubJobsScraper()
    scraper.scrape(max_jobs=50)