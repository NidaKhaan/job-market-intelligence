import pandas as pd
from database import Database
import json
from datetime import datetime

class DataLoader:
    def __init__(self):
        self.db = Database()
    
    def load_jobs_to_dataframe(self, chunk_size=1000):
        """Load jobs into pandas DataFrame efficiently"""
        print("=" * 60)
        print("📊 LOADING DATA INTO PANDAS")
        print("=" * 60)
        
        conn = self.db.get_connection()
        
        # Use chunksize for memory efficiency with large datasets
        query = """
            SELECT 
                job_id, title, company, location, 
                salary_min, salary_max, description, 
                requirements, url, source, posted_date, 
                scraped_at, is_active
            FROM jobs
            WHERE is_active = TRUE
        """
        
        print(f"\n🔄 Loading data in chunks of {chunk_size}...")
        
        # Load in chunks and concatenate
        chunks = []
        for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
            chunks.append(chunk)
        
        df = pd.concat(chunks, ignore_index=True)
        
        self.db.return_connection(conn)
        
        print(f"✅ Loaded {len(df)} jobs into DataFrame")
        print(f"📈 Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        return df
    
    def optimize_dataframe(self, df):
        """Optimize DataFrame memory usage"""
        print(f"\n🔧 Optimizing DataFrame...")
        
        initial_memory = df.memory_usage(deep=True).sum() / 1024**2
        
        # Optimize dtypes
        # Convert object columns with few unique values to category
        for col in df.select_dtypes(include=['object']).columns:
            num_unique = df[col].nunique()
            num_total = len(df[col])
            
            if num_unique / num_total < 0.5:  # Less than 50% unique
                df[col] = df[col].astype('category')
        
        final_memory = df.memory_usage(deep=True).sum() / 1024**2
        
        print(f"   • Before: {initial_memory:.2f} MB")
        print(f"   • After: {final_memory:.2f} MB")
        print(f"   • Saved: {initial_memory - final_memory:.2f} MB ({(1 - final_memory/initial_memory)*100:.1f}%)")
        
        return df
    
    def export_to_csv(self, df, filename='data/jobs_export.csv'):
        """Export DataFrame to CSV"""
        print(f"\n💾 Exporting to CSV: {filename}")
        df.to_csv(filename, index=False)
        print(f"   ✅ Exported {len(df)} jobs")
    
    def export_to_json(self, df, filename='data/jobs_export.json'):
        """Export DataFrame to JSON"""
        print(f"\n💾 Exporting to JSON: {filename}")
        df.to_json(filename, orient='records', indent=2, date_format='iso')
        print(f"   ✅ Exported {len(df)} jobs")
    
    def export_to_excel(self, df, filename='data/jobs_export.xlsx'):
        """Export DataFrame to Excel with multiple sheets"""
        print(f"\n💾 Exporting to Excel: {filename}")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main data
            df.to_excel(writer, sheet_name='All Jobs', index=False)
            
            # Summary by source
            source_summary = df.groupby('source').agg({
                'job_id': 'count',
                'salary_min': 'mean',
                'salary_max': 'mean'
            }).round(2)
            source_summary.columns = ['Job Count', 'Avg Min Salary', 'Avg Max Salary']
            source_summary.to_excel(writer, sheet_name='By Source')
            
            # Summary by location
            location_summary = df['location'].value_counts().head(20)
            location_summary.to_excel(writer, sheet_name='Top Locations')
            
            # Summary by company
            company_summary = df['company'].value_counts().head(20)
            company_summary.to_excel(writer, sheet_name='Top Companies')
        
        print(f"   ✅ Exported with multiple sheets")
    
    def get_summary_stats(self, df):
        """Generate comprehensive summary statistics"""
        print("\n" + "=" * 60)
        print("📈 DATASET SUMMARY STATISTICS")
        print("=" * 60)
        
        print(f"\n📊 Basic Info:")
        print(f"   • Total jobs: {len(df)}")
        print(f"   • Columns: {len(df.columns)}")
        print(f"   • Date range: {df['posted_date'].min()} to {df['posted_date'].max()}")
        
        print(f"\n🏢 Companies:")
        print(f"   • Unique companies: {df['company'].nunique()}")
        print(f"   • Top company: {df['company'].value_counts().index[0]} ({df['company'].value_counts().iloc[0]} jobs)")
        
        print(f"\n🌍 Locations:")
        print(f"   • Unique locations: {df['location'].nunique()}")
        print(f"   • Top 5 locations:")
        for loc, count in df['location'].value_counts().head(5).items():
            print(f"      • {loc}: {count}")
        
        print(f"\n💰 Salary Info:")
        salary_jobs = df[df['salary_min'].notna()]
        if len(salary_jobs) > 0:
            print(f"   • Jobs with salary: {len(salary_jobs)} ({len(salary_jobs)/len(df)*100:.1f}%)")
            print(f"   • Avg min salary: ${salary_jobs['salary_min'].mean():,.0f}")
            print(f"   • Avg max salary: ${salary_jobs['salary_max'].mean():,.0f}")
            print(f"   • Median min salary: ${salary_jobs['salary_min'].median():,.0f}")
        else:
            print(f"   • No salary data available")
        
        print(f"\n📚 Sources:")
        for source, count in df['source'].value_counts().items():
            print(f"   • {source}: {count} ({count/len(df)*100:.1f}%)")

if __name__ == "__main__":
    loader = DataLoader()
    
    # Load data
    df = loader.load_jobs_to_dataframe()
    
    # Optimize
    df = loader.optimize_dataframe(df)
    
    # Get stats
    loader.get_summary_stats(df)
    
    # Export to all formats
    loader.export_to_csv(df)
    loader.export_to_json(df)
    
    # Try Excel (needs openpyxl)
    try:
        loader.export_to_excel(df)
    except ImportError:
        print("\n⚠️  openpyxl not installed. Run: pip install openpyxl")