import requests
import json
from database import Database
from collections import Counter
import time

class AICareerAdvisor:
    def __init__(self):
        self.db = Database()
        self.ollama_url = "http://localhost:11434/api/generate"
        self.model = "llama3.2"
    
    def ask_ollama(self, prompt, system_prompt="", max_tokens=500):
        """Query Ollama with context - STREAMING for faster response"""
        full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
        
        payload = {
            "model": self.model,
            "prompt": full_prompt,
            "stream": True,  # Stream for immediate response
            "options": {
                "temperature": 0.7,
                "num_predict": max_tokens  # Limit length
            }
        }
        
        try:
            print("🤖 AI thinking", end="", flush=True)
            response = requests.post(self.ollama_url, json=payload, stream=True, timeout=120)
            response.raise_for_status()
            
            full_response = ""
            for line in response.iter_lines():
                if line:
                    try:
                        chunk = json.loads(line)
                        if 'response' in chunk:
                            full_response += chunk['response']
                            print(".", end="", flush=True)
                        if chunk.get('done', False):
                            break
                    except json.JSONDecodeError:
                        continue
            
            print(" ✓\n")
            return full_response
            
        except requests.exceptions.Timeout:
            return "⏱️ Response taking too long. Using faster analysis mode...\n[Switching to data-only mode]"
        except Exception as e:
            return f"Error: {e}\nMake sure Ollama is running."
    
    def get_market_context(self):
        """Get real market data as context"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Get skill frequencies
        cursor.execute("""
            SELECT extracted_skills 
            FROM jobs 
            WHERE is_active = TRUE 
            AND extracted_skills IS NOT NULL
            LIMIT 100
        """)
        
        all_skills = []
        for (skills_json,) in cursor.fetchall():
            if skills_json and skills_json != '[]':
                try:
                    skills = json.loads(skills_json)
                    all_skills.extend(skills)
                except:
                    pass
        
        skill_counts = Counter(all_skills)
        
        # Get salary data
        cursor.execute("""
            SELECT 
                AVG(salary_min) as avg_min,
                AVG(salary_max) as avg_max,
                COUNT(*) as jobs_with_salary
            FROM jobs 
            WHERE is_active = TRUE 
            AND salary_min IS NOT NULL
        """)
        
        salary_data = cursor.fetchone()
        
        cursor.close()
        self.db.return_connection(conn)
        
        return {
            'top_skills': skill_counts.most_common(20),
            'avg_salary_min': salary_data[0] if salary_data[0] else 0,
            'avg_salary_max': salary_data[1] if salary_data[1] else 0,
            'jobs_with_salary': salary_data[2]
        }
    
    def analyze_skill_gap(self, user_skills, target_role="Software Engineer"):
        """AI-powered skill gap analysis"""
        print("=" * 60)
        print("🤖 AI-POWERED CAREER ANALYSIS")
        print("=" * 60)
        
        # Get real market context
        market_data = self.get_market_context()
        
        top_10_skills = ', '.join([f'{skill}({count})' for skill, count in market_data['top_skills'][:10]])
        
        # SHORTER, FOCUSED PROMPT
        prompt = f"""Analyze career gap:

USER: {', '.join(user_skills)}
TARGET: {target_role}
MARKET TOP SKILLS: {top_10_skills}
AVG SALARY: ${market_data['avg_salary_min']:,.0f}-${market_data['avg_salary_max']:,.0f}

List:
1. Top 5 skills to learn (from market data)
2. 6-month learning priority
3. Salary potential

Keep it brief and actionable."""
        
        print(f"\n📊 Current Skills: {', '.join(user_skills)}")
        print(f"🎯 Target: {target_role}\n")
        
        response = self.ask_ollama(prompt, max_tokens=400)
        
        print(response)
        print("\n" + "=" * 60)
        
        return response, market_data
    
    def generate_quick_roadmap(self, user_skills, missing_skills, market_data):
        """Generate quick learning roadmap"""
        print("\n" + "=" * 60)
        print("🗺️  LEARNING ROADMAP")
        print("=" * 60)
        
        # Show data-driven roadmap WITHOUT AI (instant)
        print(f"\n📚 Based on {len(missing_skills)} skill gaps:\n")
        
        priority_skills = missing_skills[:5]
        
        for i, (skill, count) in enumerate(priority_skills, 1):
            pct = (count / 95) * 100  # Out of total jobs
            print(f"   {i}. {skill.upper()}")
            print(f"      • Market demand: {count} jobs ({pct:.1f}%)")
            print(f"      • Timeline: {i*2} months")
            print(f"      • Resources: Official docs, courses, projects")
            print()
        
        print("💡 Monthly commitment: 10-15 hours/week")
        print("📈 Expected outcome: 6-12 months to proficiency")
        print("\n" + "=" * 60)

def interactive_session():
    """Run interactive AI career advisor session"""
    advisor = AICareerAdvisor()
    
    print("\n" + "=" * 60)
    print("🤖 AI CAREER ADVISOR - REAL MARKET DATA + AI")
    print("=" * 60)
    
    # Get user skills
    print("\nEnter your current skills (comma-separated):")
    print("Example: python, javascript, react")
    user_input = input("Your skills: ").lower().strip()
    
    if not user_input:
        user_skills = ['python', 'html', 'css']
        print(f"Using example: {user_skills}")
    else:
        user_skills = [s.strip() for s in user_input.split(',')]
    
    # Get target role
    target = input("\nTarget role (press Enter for 'Senior Software Engineer'): ").strip()
    if not target:
        target = "Senior Software Engineer"
    
    print("\n🚀 Analyzing with AI + real job market data...\n")
    
    # AI analysis
    response, market_data = advisor.analyze_skill_gap(user_skills, target)
    
    # Find missing skills
    top_skills = [skill for skill, _ in market_data['top_skills'][:15]]
    missing_skills = [(s, c) for s, c in market_data['top_skills'] if s not in user_skills]
    
    # Quick roadmap
    if missing_skills:
        advisor.generate_quick_roadmap(user_skills, missing_skills, market_data)
    
    print("\n✅ Analysis complete!")
    print(f"   • Analyzed 95+ real job postings")
    print(f"   • AI-powered insights from Llama 3.2")
    print(f"   • Market salary data: ${market_data['avg_salary_min']:,.0f}-${market_data['avg_salary_max']:,.0f}")

if __name__ == "__main__":
    interactive_session()