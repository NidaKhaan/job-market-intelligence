import re
from collections import Counter
from database import Database
import json

class NLPProcessor:
    def __init__(self):
        self.db = Database()
        
        # Common tech skills database
        self.tech_skills = {
            'languages': ['python', 'javascript', 'java', 'c++', 'c#', 'ruby', 'go', 
                         'rust', 'swift', 'kotlin', 'typescript', 'php', 'scala', 
                         'r', 'matlab', 'sql', 'html', 'css'],
            
            'frameworks': ['react', 'angular', 'vue', 'django', 'flask', 'fastapi',
                          'spring', 'express', 'next.js', 'nuxt', 'rails', 'laravel',
                          'nest.js', 'svelte', '.net', 'asp.net'],
            
            'databases': ['postgresql', 'mysql', 'mongodb', 'redis', 'elasticsearch',
                         'cassandra', 'dynamodb', 'sqlite', 'oracle', 'sql server',
                         'mariadb', 'firestore', 'neo4j'],
            
            'cloud': ['aws', 'azure', 'gcp', 'google cloud', 'heroku', 'digitalocean',
                     'cloudflare', 'vercel', 'netlify', 'firebase'],
            
            'devops': ['docker', 'kubernetes', 'jenkins', 'terraform', 'ansible',
                      'gitlab', 'github actions', 'circleci', 'travis', 'prometheus',
                      'grafana', 'elk', 'nginx', 'apache'],
            
            'ml_ai': ['tensorflow', 'pytorch', 'scikit-learn', 'keras', 'pandas',
                     'numpy', 'opencv', 'nlp', 'machine learning', 'deep learning',
                     'neural networks', 'llm', 'gpt', 'transformers', 'hugging face'],
            
            'tools': ['git', 'jira', 'confluence', 'slack', 'figma', 'postman',
                     'vscode', 'intellij', 'linux', 'unix', 'bash', 'powershell'],
            
            'methodologies': ['agile', 'scrum', 'kanban', 'ci/cd', 'tdd', 'microservices',
                             'rest api', 'graphql', 'oauth', 'jwt', 'websocket']
        }
        
        # Flatten all skills
        self.all_skills = set()
        for category, skills in self.tech_skills.items():
            self.all_skills.update(skills)
    
    def extract_skills(self, text):
        """Extract technical skills from text"""
        if not text:
            return []
        
        text = text.lower()
        found_skills = []
        
        for skill in self.all_skills:
            # Use word boundaries for exact matches
            pattern = r'\b' + re.escape(skill) + r'\b'
            if re.search(pattern, text):
                found_skills.append(skill)
        
        return found_skills
    
    def categorize_skills(self, skills):
        """Categorize extracted skills"""
        categorized = {category: [] for category in self.tech_skills.keys()}
        
        for skill in skills:
            for category, skill_list in self.tech_skills.items():
                if skill in skill_list:
                    categorized[category].append(skill)
        
        # Remove empty categories
        categorized = {k: v for k, v in categorized.items() if v}
        
        return categorized
    
    def extract_salary_from_text(self, text):
        """Extract salary information from text"""
        if not text:
            return None, None
        
        # Common salary patterns
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})*(?:k)?)\s*-\s*\$(\d{1,3}(?:,\d{3})*(?:k)?)',  # $100k - $150k
            r'(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)',  # 100000 - 150000
            r'\$(\d{1,3})k\s*-\s*\$(\d{1,3})k',  # $100k - $150k
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                min_sal = self._parse_salary(match.group(1))
                max_sal = self._parse_salary(match.group(2))
                return min_sal, max_sal
        
        return None, None
    
    def _parse_salary(self, salary_str):
        """Parse salary string to number"""
        if not salary_str:
            return None
        
        # Remove commas and dollar signs
        salary_str = salary_str.replace(',', '').replace('$', '').lower()
        
        # Handle 'k' notation
        if 'k' in salary_str:
            try:
                return float(salary_str.replace('k', '')) * 1000
            except:
                return None
        
        try:
            return float(salary_str)
        except:
            return None
    
    def extract_experience_level(self, text):
        """Determine experience level from text"""
        if not text:
            return 'Not specified'
        
        text = text.lower()
        
        if any(word in text for word in ['senior', 'sr.', 'lead', 'principal', 'staff', 'architect']):
            return 'Senior'
        elif any(word in text for word in ['junior', 'jr.', 'entry', 'graduate', 'associate']):
            return 'Junior'
        elif any(word in text for word in ['mid-level', 'intermediate', 'mid level']):
            return 'Mid-Level'
        else:
            return 'Not specified'
    
    def process_all_jobs(self):
        """Process all jobs and extract insights"""
        print("=" * 60)
        print("🧠 NLP PROCESSING - EXTRACTING INSIGHTS")
        print("=" * 60)
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # First, add columns if they don't exist
        try:
            cursor.execute("""
                ALTER TABLE jobs 
                ADD COLUMN IF NOT EXISTS extracted_skills TEXT,
                ADD COLUMN IF NOT EXISTS experience_level VARCHAR(50)
            """)
            conn.commit()
        except:
            conn.rollback()
        
        # Get all jobs
        cursor.execute("""
            SELECT id, title, description, requirements 
            FROM jobs 
            WHERE is_active = TRUE
        """)
        
        jobs = cursor.fetchall()
        print(f"\n📊 Processing {len(jobs)} jobs...\n")
        
        all_skills = []
        experience_levels = []
        updates = []  # Store updates for batch processing
        
        for idx, (job_id, title, description, requirements) in enumerate(jobs, 1):
            # Combine text for analysis
            combined_text = f"{title or ''} {description or ''} {requirements or ''}"
            
            # Extract skills
            skills = self.extract_skills(combined_text)
            all_skills.extend(skills)
            
            # Extract experience level
            exp_level = self.extract_experience_level(title)
            experience_levels.append(exp_level)
            
            # Store for batch update
            updates.append((json.dumps(skills), exp_level, job_id))
            
            if idx % 10 == 0:
                print(f"   Processed {idx}/{len(jobs)} jobs...")
        
        # Batch update all jobs
        print(f"\n💾 Updating database...")
        try:
            cursor.executemany("""
                UPDATE jobs 
                SET extracted_skills = %s,
                    experience_level = %s
                WHERE id = %s
            """, updates)
            conn.commit()
            print(f"   ✅ Updated {len(updates)} jobs")
        except Exception as e:
            print(f"   ⚠️  Update error: {e}")
            conn.rollback()
        
        cursor.close()
        self.db.return_connection(conn)
        
        # Generate insights
        self.generate_insights(all_skills, experience_levels, len(jobs))
    
    def update_job_insights(self, job_id, skills, exp_level):
        """Update job with extracted insights"""
        pass
    
    def generate_insights(self, all_skills, experience_levels, total_jobs):
        """Generate market insights"""
        print(f"\n{'=' * 60}")
        print("📈 MARKET INSIGHTS")
        print("=" * 60)
        
        # Top skills
        skill_counts = Counter(all_skills)
        print(f"\n🔥 Top 20 In-Demand Skills:")
        for i, (skill, count) in enumerate(skill_counts.most_common(20), 1):
            percentage = (count / total_jobs) * 100
            print(f"   {i:2d}. {skill:20s} - {count:3d} jobs ({percentage:5.1f}%)")
        
        # Experience level distribution
        exp_counts = Counter(experience_levels)
        print(f"\n👔 Experience Level Distribution:")
        for level, count in exp_counts.most_common():
            percentage = (count / total_jobs) * 100
            print(f"   • {level:20s} - {count:3d} jobs ({percentage:5.1f}%)")
        
        # Skill categories
        print(f"\n📚 Skills by Category:")
        categorized = self.categorize_skills(list(skill_counts.keys()))
        for category, skills in categorized.items():
            print(f"   • {category:20s} - {len(skills)} unique skills")
        
        print("\n" + "=" * 60)

if __name__ == "__main__":
    processor = NLPProcessor()
    processor.process_all_jobs()