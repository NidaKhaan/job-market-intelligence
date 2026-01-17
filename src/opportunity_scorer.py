from database import Database
import json
from collections import Counter
import pandas as pd

class OpportunityScorer:
    def __init__(self):
        self.db = Database()
    
    def score_job(self, job_id, user_skills, preferences=None):
        """Score a job opportunity based on multiple factors"""
        if preferences is None:
            preferences = {
                'min_salary': 80000,
                'preferred_location': 'Remote',
                'required_skills': [],
                'nice_to_have_skills': [],
                'experience_level': 'Senior'
            }
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Get job details
        cursor.execute("""
            SELECT 
                job_id, title, company, location, 
                salary_min, salary_max, extracted_skills,
                experience_level, requirements, description,
                source, url
            FROM jobs 
            WHERE id = %s
        """, (job_id,))
        
        job = cursor.fetchone()
        
        if not job:
            return None
        
        cursor.close()
        self.db.return_connection(conn)
        
        # Unpack job data
        (job_uid, title, company, location, salary_min, salary_max, 
         extracted_skills_json, exp_level, requirements, description,
         source, url) = job
        
        # Parse skills
        job_skills = []
        if extracted_skills_json:
            try:
                job_skills = json.loads(extracted_skills_json)
            except:
                pass
        
        # Calculate scores (0-100 for each category)
        scores = {}
        
        # 1. Skill Match Score (40% weight)
        scores['skill_match'] = self.calculate_skill_match(user_skills, job_skills)
        
        # 2. Salary Score (25% weight)
        scores['salary'] = self.calculate_salary_score(
            salary_min, salary_max, preferences['min_salary']
        )
        
        # 3. Location Score (15% weight)
        scores['location'] = self.calculate_location_score(
            location, preferences['preferred_location']
        )
        
        # 4. Experience Level Match (10% weight)
        scores['experience'] = self.calculate_experience_score(
            exp_level, preferences['experience_level']
        )
        
        # 5. Company Growth Score (10% weight)
        scores['company_growth'] = self.calculate_company_score(company)
        
        # Calculate weighted total
        weights = {
            'skill_match': 0.40,
            'salary': 0.25,
            'location': 0.15,
            'experience': 0.10,
            'company_growth': 0.10
        }
        
        total_score = sum(scores[key] * weights[key] for key in scores)
        
        return {
            'job_id': job_uid,
            'title': title,
            'company': company,
            'location': location,
            'salary_range': f"${salary_min:,.0f} - ${salary_max:,.0f}" if salary_min else "Not disclosed",
            'total_score': round(total_score, 1),
            'breakdown': scores,
            'matching_skills': [s for s in user_skills if s in job_skills],
            'missing_skills': [s for s in job_skills if s not in user_skills],
            'url': url,
            'recommendation': self.get_recommendation(total_score)
        }
    
    def calculate_skill_match(self, user_skills, job_skills):
        """Calculate skill match percentage"""
        if not job_skills:
            return 50  # Neutral if no skills listed
        
        matching = len([s for s in user_skills if s in job_skills])
        total_required = len(job_skills)
        
        if total_required == 0:
            return 50
        
        match_percentage = (matching / total_required) * 100
        
        # Bonus for having more skills than required
        if matching >= total_required:
            match_percentage = min(100, match_percentage + 10)
        
        return round(match_percentage, 1)
    
    def calculate_salary_score(self, salary_min, salary_max, min_desired):
        """Score based on salary"""
        if not salary_min or not salary_max:
            return 50  # Unknown salary = neutral
        
        avg_salary = (salary_min + salary_max) / 2
        
        if avg_salary >= min_desired * 1.5:
            return 100  # 50%+ above desired
        elif avg_salary >= min_desired * 1.2:
            return 85   # 20%+ above desired
        elif avg_salary >= min_desired:
            return 70   # Meets desired
        elif avg_salary >= min_desired * 0.8:
            return 50   # Close to desired
        else:
            return 30   # Below desired
    
    def calculate_location_score(self, job_location, preferred_location):
        """Score based on location match"""
        if not job_location:
            return 50
        
        job_loc_lower = job_location.lower()
        pref_loc_lower = preferred_location.lower()
        
        if 'remote' in job_loc_lower and 'remote' in pref_loc_lower:
            return 100  # Perfect match
        elif pref_loc_lower in job_loc_lower or job_loc_lower in pref_loc_lower:
            return 90   # Close match
        elif 'remote' in job_loc_lower:
            return 80   # Remote is flexible
        else:
            return 40   # Different location
    
    def calculate_experience_score(self, job_exp_level, user_exp_level):
        """Score based on experience level match"""
        if not job_exp_level or job_exp_level == 'Not specified':
            return 70  # Flexible
        
        exp_levels = ['Junior', 'Mid-Level', 'Senior']
        
        try:
            job_idx = exp_levels.index(job_exp_level)
            user_idx = exp_levels.index(user_exp_level)
            
            if job_idx == user_idx:
                return 100  # Perfect match
            elif abs(job_idx - user_idx) == 1:
                return 70   # One level off
            else:
                return 40   # Two levels off
        except ValueError:
            return 70  # Can't determine
    
    def calculate_company_score(self, company):
        """Score based on company hiring velocity"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # How many jobs does this company have?
        cursor.execute("""
            SELECT COUNT(*) 
            FROM jobs 
            WHERE company = %s AND is_active = TRUE
        """, (company,))
        
        job_count = cursor.fetchone()[0]
        
        cursor.close()
        self.db.return_connection(conn)
        
        # More jobs = growing company
        if job_count >= 5:
            return 90   # Rapidly hiring
        elif job_count >= 3:
            return 75   # Actively hiring
        elif job_count >= 2:
            return 60   # Some growth
        else:
            return 50   # Single posting
    
    def get_recommendation(self, score):
        """Get recommendation based on score"""
        if score >= 80:
            return "🔥 HIGHLY RECOMMENDED - Apply immediately!"
        elif score >= 65:
            return "✅ GOOD FIT - Strong candidate"
        elif score >= 50:
            return "🤔 CONSIDER - May need to strengthen application"
        else:
            return "⚠️ NOT IDEAL - Look for better matches"
    
    def rank_all_jobs(self, user_skills, preferences=None, limit=20):
        """Rank all jobs for a user"""
        print("=" * 60)
        print("🎯 JOB OPPORTUNITY RANKING")
        print("=" * 60)
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM jobs WHERE is_active = TRUE")
        job_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        self.db.return_connection(conn)
        
        print(f"\n📊 Analyzing {len(job_ids)} opportunities...")
        print(f"👤 Your skills: {', '.join(user_skills)}\n")
        
        # Score all jobs
        scored_jobs = []
        for job_id in job_ids:
            score_data = self.score_job(job_id, user_skills, preferences)
            if score_data:
                scored_jobs.append(score_data)
        
        # Sort by score
        scored_jobs.sort(key=lambda x: x['total_score'], reverse=True)
        
        # Display top matches
        print(f"🏆 TOP {min(limit, len(scored_jobs))} MATCHES:\n")
        
        for i, job in enumerate(scored_jobs[:limit], 1):
            print(f"{i:2d}. [{job['total_score']:5.1f}%] {job['title'][:45]}")
            print(f"    Company: {job['company'][:40]}")
            print(f"    Location: {job['location'][:40]}")
            print(f"    Salary: {job['salary_range']}")
            print(f"    Match: {len(job['matching_skills'])}/{len(job['matching_skills']) + len(job['missing_skills'])} skills")
            print(f"    {job['recommendation']}")
            
            if job['missing_skills'][:3]:
                print(f"    Missing: {', '.join(job['missing_skills'][:3])}")
            
            print(f"    URL: {job['url'][:60]}...")
            print()
        
        return scored_jobs
    
    def show_detailed_analysis(self, job_score):
        """Show detailed breakdown of a job score"""
        print("\n" + "=" * 60)
        print(f"📋 DETAILED ANALYSIS: {job_score['title']}")
        print("=" * 60)
        
        print(f"\n🏢 Company: {job_score['company']}")
        print(f"📍 Location: {job_score['location']}")
        print(f"💰 Salary: {job_score['salary_range']}")
        
        print(f"\n📊 SCORE BREAKDOWN:")
        breakdown = job_score['breakdown']
        print(f"   • Skill Match:      {breakdown['skill_match']:5.1f}% (40% weight)")
        print(f"   • Salary Score:     {breakdown['salary']:5.1f}% (25% weight)")
        print(f"   • Location Match:   {breakdown['location']:5.1f}% (15% weight)")
        print(f"   • Experience Level: {breakdown['experience']:5.1f}% (10% weight)")
        print(f"   • Company Growth:   {breakdown['company_growth']:5.1f}% (10% weight)")
        print(f"   ─────────────────────────────────")
        print(f"   TOTAL SCORE:        {job_score['total_score']:5.1f}%")
        
        print(f"\n✅ Skills You Have ({len(job_score['matching_skills'])}):")
        for skill in job_score['matching_skills']:
            print(f"   • {skill}")
        
        print(f"\n📚 Skills to Learn ({len(job_score['missing_skills'])}):")
        for skill in job_score['missing_skills'][:10]:
            print(f"   • {skill}")
        
        print(f"\n{job_score['recommendation']}")
        print(f"\n🔗 Apply: {job_score['url']}")

if __name__ == "__main__":
    scorer = OpportunityScorer()
    
    # Example user profile
    print("🎯 JOB OPPORTUNITY SCORER\n")
    
    user_input = input("Enter your skills (comma-separated): ").lower().strip()
    if not user_input:
        user_skills = ['python', 'kubernetes', 'docker', 'aws']
        print(f"Using example: {user_skills}")
    else:
        user_skills = [s.strip() for s in user_input.split(',')]
    
    # User preferences
    preferences = {
        'min_salary': 100000,
        'preferred_location': 'Remote',
        'experience_level': 'Senior'
    }
    
    # Rank all jobs
    scored_jobs = scorer.rank_all_jobs(user_skills, preferences, limit=15)
    
    # Show detailed analysis of top job
    if scored_jobs:
        print("\n" + "=" * 60)
        choice = input("\nShow detailed analysis of top job? (y/n): ").lower()
        if choice == 'y':
            scorer.show_detailed_analysis(scored_jobs[0])