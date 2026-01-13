import re
from datetime import datetime
from database import Database
import pandas as pd

class DataProcessor:
    def __init__(self):
        self.db = Database()
        self.cleaned_count = 0
        self.errors = []
    
    def clean_all_jobs(self):
        """Main cleaning pipeline"""
        print("=" * 60)
        print("🧹 DATA CLEANING PIPELINE")
        print("=" * 60)
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Fetch all jobs
        cursor.execute("SELECT * FROM jobs")
        jobs = cursor.fetchall()
        
        print(f"\n📊 Processing {len(jobs)} jobs...")
        
        for job in jobs:
            try:
                cleaned_job = self.clean_job(job)
                if cleaned_job:
                    self.update_job(cleaned_job)
                    self.cleaned_count += 1
            except Exception as e:
                self.errors.append(f"Job ID {job[0]}: {e}")
        
        cursor.close()
        self.db.return_connection(conn)
        
        self.print_summary()
    
    def clean_job(self, job):
        """Clean individual job record"""
        # job tuple: (id, job_id, title, company, location, salary_min, salary_max, 
        #             description, requirements, url, source, posted_date, scraped_at, is_active)
        
        job_dict = {
            'id': job[0],
            'job_id': job[1],
            'title': self.clean_title(job[2]),
            'company': self.clean_company(job[3]),
            'location': self.standardize_location(job[4]),
            'salary_min': job[5],
            'salary_max': job[6],
            'description': self.clean_text(job[7]),
            'requirements': self.clean_requirements(job[8]),
            'url': job[9],
            'source': job[10],
            'posted_date': job[11],
            'scraped_at': job[12],
            'is_active': job[13]
        }
        
        return job_dict
    
    def clean_title(self, title):
        """Standardize job titles"""
        if not title or title == 'N/A':
            return 'Unknown Position'
        
        # Remove extra whitespace
        title = ' '.join(title.split())
        
        # Remove special characters but keep common ones
        title = re.sub(r'[^\w\s\-/().,&+#]', '', title)
        
        # Capitalize properly
        title = title.title()
        
        return title.strip()
    
    def clean_company(self, company):
        """Standardize company names"""
        if not company or company == 'N/A':
            return 'Unknown Company'
        
        # Remove extra whitespace
        company = ' '.join(company.split())
        
        # Remove common suffixes for consistency (optional)
        # company = re.sub(r',?\s*(Inc\.?|LLC|Ltd\.?|Corporation|Corp\.?)$', '', company, flags=re.IGNORECASE)
        
        return company.strip()
    
    def standardize_location(self, location):
        """Standardize location formats"""
        if not location:
            return 'Remote - Worldwide'
        
        location = ' '.join(location.split())
        
        # Common patterns
        if re.search(r'(anywhere|worldwide|global)', location, re.IGNORECASE):
            return 'Remote - Worldwide'
        elif re.search(r'(usa|united states|us only)', location, re.IGNORECASE):
            return 'Remote - USA'
        elif re.search(r'(europe|eu)', location, re.IGNORECASE):
            return 'Remote - Europe'
        elif location.lower() == 'remote':
            return 'Remote - Worldwide'
        
        return location.strip()
    
    def clean_text(self, text):
        """Clean description and long text fields"""
        if not text:
            return ''
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove HTML tags if any
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove special characters but keep punctuation
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', text)
        
        return text.strip()
    
    def clean_requirements(self, requirements):
        """Standardize requirements/skills"""
        if not requirements:
            return ''
        
        # Split by comma, clean each, remove duplicates
        skills = [skill.strip().lower() for skill in requirements.split(',')]
        skills = list(dict.fromkeys(skills))  # Remove duplicates while preserving order
        
        # Remove empty strings
        skills = [s for s in skills if s]
        
        # Limit to top 20 skills
        skills = skills[:20]
        
        return ', '.join(skills)
    
    def update_job(self, job_dict):
        """Update cleaned job in database"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE jobs 
                SET title = %s,
                    company = %s,
                    location = %s,
                    description = %s,
                    requirements = %s
                WHERE id = %s
            """, (
                job_dict['title'],
                job_dict['company'],
                job_dict['location'],
                job_dict['description'],
                job_dict['requirements'],
                job_dict['id']
            ))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            self.db.return_connection(conn)
    
    def print_summary(self):
        """Print cleaning summary"""
        print(f"\n✅ Cleaning Complete!")
        print(f"   • Jobs processed: {self.cleaned_count}")
        print(f"   • Errors: {len(self.errors)}")
        
        if self.errors:
            print(f"\n⚠️  Errors encountered:")
            for error in self.errors[:5]:  # Show first 5
                print(f"   • {error}")

if __name__ == "__main__":
    processor = DataProcessor()
    processor.clean_all_jobs()