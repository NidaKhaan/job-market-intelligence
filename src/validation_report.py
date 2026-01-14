import pandas as pd
from database import Database
from datetime import datetime
import json

class ValidationReport:
    def __init__(self):
        self.db = Database()
        self.issues = []
    
    def generate_report(self):
        """Generate comprehensive validation report"""
        print("=" * 60)
        print("🔍 DATA VALIDATION REPORT")
        print("=" * 60)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        conn = self.db.get_connection()
        df = pd.read_sql("SELECT * FROM jobs WHERE is_active = TRUE", conn)
        self.db.return_connection(conn)
        
        # Run all validation checks
        self.check_completeness(df)
        self.check_data_types(df)
        self.check_salary_consistency(df)
        self.check_url_validity(df)
        self.check_text_quality(df)
        
        # Summary
        self.print_summary()
        
        # Save report
        self.save_report()
    
    def check_completeness(self, df):
        """Check for missing or invalid data"""
        print("📋 COMPLETENESS CHECK")
        print("-" * 60)
        
        for col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100
            
            if null_pct > 0:
                print(f"   • {col}: {null_count} missing ({null_pct:.1f}%)")
                if null_pct > 10:  # Flag if > 10%
                    self.issues.append(f"High missing rate in {col}: {null_pct:.1f}%")
        
        print()
    
    def check_data_types(self, df):
        """Validate data types"""
        print("🔤 DATA TYPE CHECK")
        print("-" * 60)
        
        # Check salary fields are numeric
        if df['salary_min'].dtype not in ['float64', 'int64']:
            self.issues.append("salary_min is not numeric")
            print("   ⚠️  salary_min is not numeric")
        
        if df['salary_max'].dtype not in ['float64', 'int64']:
            self.issues.append("salary_max is not numeric")
            print("   ⚠️  salary_max is not numeric")
        
        # Check dates
        try:
            pd.to_datetime(df['posted_date'])
            print("   ✅ posted_date format valid")
        except:
            self.issues.append("Invalid posted_date format")
            print("   ⚠️  posted_date format invalid")
        
        print()
    
    def check_salary_consistency(self, df):
        """Check salary logic"""
        print("💰 SALARY CONSISTENCY CHECK")
        print("-" * 60)
        
        salary_df = df[df['salary_min'].notna() & df['salary_max'].notna()]
        
        if len(salary_df) > 0:
            # Check if min > max
            invalid = salary_df[salary_df['salary_min'] > salary_df['salary_max']]
            
            if len(invalid) > 0:
                print(f"   ⚠️  {len(invalid)} jobs have min salary > max salary")
                self.issues.append(f"{len(invalid)} jobs with invalid salary range")
            else:
                print("   ✅ All salary ranges valid")
            
            # Check for outliers
            avg_min = salary_df['salary_min'].mean()
            outliers = salary_df[salary_df['salary_min'] > avg_min * 3]
            
            if len(outliers) > 0:
                print(f"   ℹ️  {len(outliers)} potential salary outliers (>3x average)")
        else:
            print("   ℹ️  No salary data to validate")
        
        print()
    
    def check_url_validity(self, df):
        """Check URL formats"""
        print("🔗 URL VALIDITY CHECK")
        print("-" * 60)
        
        import re
        url_pattern = re.compile(r'https?://[^\s]+')
        
        invalid_urls = 0
        for url in df['url']:
            if pd.notna(url) and not url_pattern.match(str(url)):
                invalid_urls += 1
        
        if invalid_urls > 0:
            print(f"   ⚠️  {invalid_urls} invalid URLs")
            self.issues.append(f"{invalid_urls} invalid URLs")
        else:
            print("   ✅ All URLs valid")
        
        print()
    
    def check_text_quality(self, df):
        """Check text field quality"""
        print("📝 TEXT QUALITY CHECK")
        print("-" * 60)
        
        # Check for very short descriptions
        if 'description' in df.columns:
            short_desc = df[df['description'].str.len() < 50]
            if len(short_desc) > 0:
                print(f"   ℹ️  {len(short_desc)} jobs with short descriptions (<50 chars)")
        
        # Check for very long requirements
        if 'requirements' in df.columns:
            long_req = df[df['requirements'].str.len() > 1000]
            if len(long_req) > 0:
                print(f"   ℹ️  {len(long_req)} jobs with very long requirements (>1000 chars)")
        
        print("   ✅ Text quality checks complete")
        print()
    
    def print_summary(self):
        """Print validation summary"""
        print("=" * 60)
        print("📊 VALIDATION SUMMARY")
        print("=" * 60)
        
        if len(self.issues) == 0:
            print("\n✅ ALL CHECKS PASSED! Data quality is excellent.")
        else:
            print(f"\n⚠️  Found {len(self.issues)} issues:\n")
            for i, issue in enumerate(self.issues, 1):
                print(f"   {i}. {issue}")
        
        print("\n" + "=" * 60)
    
    def save_report(self):
        """Save report to file"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'issues_count': len(self.issues),
            'issues': self.issues,
            'status': 'PASSED' if len(self.issues) == 0 else 'NEEDS ATTENTION'
        }
        
        filename = f"data/validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n💾 Report saved: {filename}")

if __name__ == "__main__":
    validator = ValidationReport()
    validator.generate_report()