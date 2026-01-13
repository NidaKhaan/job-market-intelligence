from database import Database
from difflib import SequenceMatcher

class DuplicateDetector:
    def __init__(self):
        self.db = Database()
        self.duplicates_found = 0
    
    def find_duplicates(self, similarity_threshold=0.85):
        """Find potential duplicate jobs"""
        print("=" * 60)
        print("🔍 DUPLICATE DETECTION")
        print("=" * 60)
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Get all jobs
        cursor.execute("""
            SELECT id, job_id, title, company, url 
            FROM jobs 
            WHERE is_active = TRUE
            ORDER BY scraped_at DESC
        """)
        
        jobs = cursor.fetchall()
        print(f"\n📊 Analyzing {len(jobs)} jobs...")
        
        duplicates = []
        checked = set()
        
        for i, job1 in enumerate(jobs):
            for job2 in jobs[i+1:]:
                # Skip if already checked
                pair = tuple(sorted([job1[0], job2[0]]))
                if pair in checked:
                    continue
                checked.add(pair)
                
                # Check if same source and ID (exact duplicate)
                if job1[1] == job2[1]:
                    duplicates.append({
                        'job1_id': job1[0],
                        'job2_id': job2[0],
                        'reason': 'Exact ID match',
                        'similarity': 1.0
                    })
                    continue
                
                # Check title + company similarity
                if job1[3] == job2[3]:  # Same company
                    title_similarity = self.similarity(job1[2], job2[2])
                    
                    if title_similarity >= similarity_threshold:
                        duplicates.append({
                            'job1_id': job1[0],
                            'job2_id': job2[0],
                            'reason': f'Similar title at same company',
                            'similarity': title_similarity,
                            'title1': job1[2],
                            'title2': job2[2],
                            'company': job1[3]
                        })
        
        print(f"\n⚠️  Found {len(duplicates)} potential duplicates")
        
        if duplicates:
            print(f"\n📝 Sample duplicates:")
            for dup in duplicates[:5]:
                print(f"\n   Similarity: {dup['similarity']:.2%}")
                print(f"   Reason: {dup['reason']}")
                if 'title1' in dup:
                    print(f"   Title 1: {dup['title1']}")
                    print(f"   Title 2: {dup['title2']}")
                    print(f"   Company: {dup['company']}")
        
        cursor.close()
        self.db.return_connection(conn)
        
        return duplicates
    
    def similarity(self, str1, str2):
        """Calculate similarity between two strings"""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def mark_duplicate(self, job_id):
        """Mark a job as inactive (soft delete)"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE jobs 
            SET is_active = FALSE 
            WHERE id = %s
        """, (job_id,))
        
        conn.commit()
        cursor.close()
        self.db.return_connection(conn)

if __name__ == "__main__":
    detector = DuplicateDetector()
    detector.find_duplicates()