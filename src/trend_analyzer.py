from database import Database
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
import json

class TrendAnalyzer:
    def __init__(self):
        self.db = Database()
    
    def analyze_skill_trends(self, days_back=30):
        """Analyze skill demand trends over time"""
        print("=" * 60)
        print("📈 SKILL TREND ANALYSIS")
        print("=" * 60)
        
        conn = self.db.get_connection()
        
        # Get jobs from different time periods
        query = """
            SELECT 
                extracted_skills,
                scraped_at,
                DATE(scraped_at) as scrape_date
            FROM jobs 
            WHERE is_active = TRUE 
            AND extracted_skills IS NOT NULL
            AND scraped_at >= NOW() - INTERVAL '%s days'
            ORDER BY scraped_at DESC
        """
        
        df = pd.read_sql(query % days_back, conn)
        self.db.return_connection(conn)
        
        print(f"\n📊 Analyzing {len(df)} jobs from last {days_back} days")
        
        if len(df) == 0:
            print("⚠️  No data in selected time range")
            return
        
        # Group by date
        dates = df['scrape_date'].unique()
        print(f"   • Date range: {dates.min()} to {dates.max()}")
        print(f"   • Unique dates: {len(dates)}")
        
        # Analyze skills by time period
        self.skill_growth_analysis(df)
        
        return df
    
    def skill_growth_analysis(self, df):
        """Detect growing vs declining skills"""
        print(f"\n🔥 SKILL GROWTH ANALYSIS")
        print("-" * 60)
        
        # Split into recent vs older
        mid_point = df['scraped_at'].median()
        
        recent_jobs = df[df['scraped_at'] >= mid_point]
        older_jobs = df[df['scraped_at'] < mid_point]
        
        print(f"   • Recent period: {len(recent_jobs)} jobs")
        print(f"   • Earlier period: {len(older_jobs)} jobs")
        
        # Extract skills from each period
        recent_skills = self.extract_all_skills(recent_jobs)
        older_skills = self.extract_all_skills(older_jobs)
        
        # Calculate frequencies
        recent_freq = Counter(recent_skills)
        older_freq = Counter(older_skills)
        
        # Find all unique skills
        all_skills = set(list(recent_freq.keys()) + list(older_freq.keys()))
        
        # Calculate growth rates
        growth_data = []
        for skill in all_skills:
            recent_count = recent_freq.get(skill, 0)
            older_count = older_freq.get(skill, 0)
            
            if older_count > 0:
                growth_rate = ((recent_count - older_count) / older_count) * 100
            else:
                growth_rate = 100 if recent_count > 0 else 0
            
            growth_data.append({
                'skill': skill,
                'recent_count': recent_count,
                'older_count': older_count,
                'growth_rate': growth_rate,
                'total_mentions': recent_count + older_count
            })
        
        # Sort by growth rate
        growth_data.sort(key=lambda x: x['growth_rate'], reverse=True)
        
        # Show trending up
        print(f"\n🚀 TRENDING UP (Fastest Growing):")
        for item in growth_data[:10]:
            if item['total_mentions'] >= 2:  # Filter noise
                print(f"   • {item['skill']:20s} "
                      f"📈 {item['growth_rate']:+6.1f}% "
                      f"({item['older_count']} → {item['recent_count']})")
        
        # Show trending down
        print(f"\n📉 TRENDING DOWN:")
        declining = [x for x in growth_data if x['growth_rate'] < -20 and x['total_mentions'] >= 2]
        for item in declining[-10:]:
            print(f"   • {item['skill']:20s} "
                  f"📉 {item['growth_rate']:+6.1f}% "
                  f"({item['older_count']} → {item['recent_count']})")
        
        # Show stable/consistent
        print(f"\n⚖️  CONSISTENTLY IN DEMAND:")
        stable = [x for x in growth_data if abs(x['growth_rate']) < 20 and x['total_mentions'] >= 5]
        stable.sort(key=lambda x: x['total_mentions'], reverse=True)
        for item in stable[:10]:
            print(f"   • {item['skill']:20s} "
                  f"Stable: {item['total_mentions']} mentions")
    
    def extract_all_skills(self, df):
        """Extract all skills from jobs"""
        all_skills = []
        for skills_json in df['extracted_skills']:
            if skills_json and skills_json != '[]':
                try:
                    skills = json.loads(skills_json)
                    all_skills.extend(skills)
                except:
                    pass
        return all_skills
    
    def analyze_company_hiring_patterns(self):
        """Analyze which companies are hiring most"""
        print(f"\n{'=' * 60}")
        print("🏢 COMPANY HIRING PATTERNS")
        print("=" * 60)
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Companies with most jobs
        cursor.execute("""
            SELECT company, COUNT(*) as job_count,
                   COUNT(DISTINCT DATE(scraped_at)) as active_days
            FROM jobs
            WHERE is_active = TRUE
            GROUP BY company
            HAVING COUNT(*) >= 2
            ORDER BY job_count DESC
            LIMIT 15
        """)
        
        print(f"\n📊 Top Hiring Companies:")
        for company, count, days in cursor.fetchall():
            rate = count / days if days > 0 else count
            print(f"   • {company[:35]:35s} - {count:2d} jobs ({rate:.1f}/day)")
        
        cursor.close()
        self.db.return_connection(conn)
    
    def analyze_location_trends(self):
        """Analyze geographic distribution"""
        print(f"\n{'=' * 60}")
        print("🌍 LOCATION TRENDS")
        print("=" * 60)
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                location,
                COUNT(*) as job_count,
                AVG(salary_min) as avg_min,
                AVG(salary_max) as avg_max
            FROM jobs
            WHERE is_active = TRUE
            GROUP BY location
            HAVING COUNT(*) >= 2
            ORDER BY job_count DESC
            LIMIT 10
        """)
        
        print(f"\n📍 Top Locations:")
        for location, count, avg_min, avg_max in cursor.fetchall():
            salary_info = ""
            if avg_min and avg_max:
                salary_info = f" | Avg: ${avg_min:,.0f}-${avg_max:,.0f}"
            print(f"   • {location[:30]:30s} - {count:2d} jobs{salary_info}")
        
        cursor.close()
        self.db.return_connection(conn)
    
    def generate_market_report(self):
        """Generate comprehensive market intelligence report"""
        print("\n" + "=" * 60)
        print("📋 COMPREHENSIVE MARKET INTELLIGENCE REPORT")
        print("=" * 60)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Overall stats
        cursor.execute("SELECT COUNT(*) FROM jobs WHERE is_active = TRUE")
        total_jobs = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM jobs 
            WHERE is_active = TRUE 
            AND scraped_at >= NOW() - INTERVAL '24 hours'
        """)
        jobs_24h = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT company) FROM jobs WHERE is_active = TRUE
        """)
        unique_companies = cursor.fetchone()[0]
        
        print(f"📊 Market Overview:")
        print(f"   • Total active jobs: {total_jobs}")
        print(f"   • New in last 24h: {jobs_24h}")
        print(f"   • Unique companies: {unique_companies}")
        print(f"   • Data sources: 2 (RemoteOK, Remotive)")
        
        # Market velocity
        cursor.execute("""
            SELECT 
                DATE(scraped_at) as date,
                COUNT(*) as count
            FROM jobs
            WHERE scraped_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(scraped_at)
            ORDER BY date DESC
            LIMIT 7
        """)
        
        print(f"\n📈 7-Day Activity:")
        for date, count in cursor.fetchall():
            print(f"   • {date}: {count} jobs")
        
        cursor.close()
        self.db.return_connection(conn)

if __name__ == "__main__":
    analyzer = TrendAnalyzer()
    
    # Run all analyses
    analyzer.analyze_skill_trends(days_back=30)
    analyzer.analyze_company_hiring_patterns()
    analyzer.analyze_location_trends()
    analyzer.generate_market_report()