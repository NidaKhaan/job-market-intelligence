import pandas as pd
import numpy as np
from database import Database
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
import json
import pickle
from datetime import datetime

class SalaryPredictor:
    def __init__(self):
        self.db = Database()
        self.model = None
        self.encoders = {}
        self.feature_importance = {}
    
    def load_data(self):
        """Load job data for training"""
        print("=" * 60)
        print("💰 SALARY PREDICTION ENGINE")
        print("=" * 60)
        
        conn = self.db.get_connection()
        
        query = """
            SELECT 
                title, company, location, 
                salary_min, salary_max, 
                extracted_skills, experience_level,
                source
            FROM jobs 
            WHERE is_active = TRUE 
            AND salary_min IS NOT NULL 
            AND salary_max IS NOT NULL
        """
        
        df = pd.read_sql(query, conn)
        self.db.return_connection(conn)
        
        print(f"\n📊 Loaded {len(df)} jobs with salary data")
        
        return df
    
    def prepare_features(self, df):
        """Prepare features for ML model"""
        print(f"\n🔧 Preparing features...")
        
        # Create average salary as target
        df['avg_salary'] = (df['salary_min'] + df['salary_max']) / 2
        
        # Extract skill count
        df['skill_count'] = df['extracted_skills'].apply(
            lambda x: len(json.loads(x)) if x and x != '[]' else 0
        )
        
        # Has specific valuable skills
        def has_skill(skills_json, skill):
            if not skills_json or skills_json == '[]':
                return 0
            skills = json.loads(skills_json)
            return 1 if skill in skills else 0
        
        # Add valuable skill flags
        valuable_skills = ['python', 'kubernetes', 'aws', 'react', 'go', 
                          'terraform', 'docker', 'machine learning']
        
        for skill in valuable_skills:
            df[f'has_{skill.replace(" ", "_")}'] = df['extracted_skills'].apply(
                lambda x: has_skill(x, skill)
            )
        
        # Title features
        df['title_length'] = df['title'].str.len()
        df['is_senior'] = df['title'].str.lower().str.contains('senior|lead|principal|staff').astype(int)
        df['is_engineer'] = df['title'].str.lower().str.contains('engineer|developer').astype(int)
        
        # Encode categorical variables
        if 'experience_level' not in self.encoders:
            self.encoders['experience_level'] = LabelEncoder()
            df['exp_level_encoded'] = self.encoders['experience_level'].fit_transform(
                df['experience_level'].fillna('Not specified')
            )
        else:
            df['exp_level_encoded'] = self.encoders['experience_level'].transform(
                df['experience_level'].fillna('Not specified')
            )
        
        if 'location' not in self.encoders:
            self.encoders['location'] = LabelEncoder()
            df['location_encoded'] = self.encoders['location'].fit_transform(
                df['location'].fillna('Remote')
            )
        else:
            df['location_encoded'] = self.encoders['location'].transform(
                df['location'].fillna('Remote')
            )
        
        print(f"   ✅ Created {len(df.columns)} features")
        
        return df
    
    def train_model(self, df):
        """Train salary prediction model"""
        print(f"\n🤖 Training prediction model...")
        
        # Select features
        feature_cols = ['skill_count', 'title_length', 'is_senior', 'is_engineer',
                       'exp_level_encoded', 'location_encoded']
        
        # Add skill flags
        feature_cols.extend([col for col in df.columns if col.startswith('has_')])
        
        X = df[feature_cols]
        y = df['avg_salary']
        
        print(f"   • Features: {len(feature_cols)}")
        print(f"   • Training samples: {len(X)}")
        
        if len(X) < 10:
            print(f"\n   ⚠️  Too few samples ({len(X)}) for reliable training")
            print(f"   💡 Need at least 10 jobs with salary data")
            print(f"   📊 Current model will have limited accuracy")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Train model
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
        
        self.model.fit(X_train, y_train)
        
        # Evaluate
        train_score = self.model.score(X_train, y_train)
        test_score = self.model.score(X_test, y_test)
        
        print(f"   ✅ Model trained!")
        print(f"   • Training R²: {train_score:.3f}")
        print(f"   • Testing R²: {test_score:.3f}")
        
        # Feature importance
        self.feature_importance = dict(zip(feature_cols, self.model.feature_importances_))
        
        return X_test, y_test
    
    def show_predictions(self, X_test, y_test):
        """Show sample predictions"""
        print(f"\n📈 Sample Predictions:")
        print("-" * 60)
        
        predictions = self.model.predict(X_test)
        
        for i in range(min(5, len(X_test))):
            actual = y_test.iloc[i]
            predicted = predictions[i]
            error = abs(actual - predicted)
            error_pct = (error / actual) * 100
            
            print(f"   {i+1}. Actual: ${actual:,.0f} | "
                  f"Predicted: ${predicted:,.0f} | "
                  f"Error: {error_pct:.1f}%")
    
    def show_feature_importance(self):
        """Show most important features"""
        print(f"\n🎯 Feature Importance:")
        print("-" * 60)
        
        sorted_features = sorted(
            self.feature_importance.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        for i, (feature, importance) in enumerate(sorted_features[:10], 1):
            print(f"   {i:2d}. {feature:30s} - {importance:.4f}")
    
    def predict_salary(self, job_title, skills, location='Remote', experience='Not specified'):
        """Predict salary for a job posting"""
        if not self.model:
            return None
        
        # Create feature dict
        features = {
            'skill_count': len(skills),
            'title_length': len(job_title),
            'is_senior': 1 if any(word in job_title.lower() for word in ['senior', 'lead', 'principal']) else 0,
            'is_engineer': 1 if any(word in job_title.lower() for word in ['engineer', 'developer']) else 0,
            'exp_level_encoded': self.encoders['experience_level'].transform([experience])[0],
            'location_encoded': self.encoders['location'].transform([location])[0]
        }
        
        # Add skill flags
        valuable_skills = ['python', 'kubernetes', 'aws', 'react', 'go', 
                          'terraform', 'docker', 'machine learning']
        for skill in valuable_skills:
            features[f'has_{skill.replace(" ", "_")}'] = 1 if skill in skills else 0
        
        # Create DataFrame
        X = pd.DataFrame([features])
        
        # Predict
        prediction = self.model.predict(X)[0]
        
        return prediction
    
    def save_model(self, filename='data/salary_model.pkl'):
        """Save trained model"""
        model_data = {
            'model': self.model,
            'encoders': self.encoders,
            'feature_importance': self.feature_importance,
            'trained_at': datetime.now().isoformat()
        }
        
        with open(filename, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"\n💾 Model saved: {filename}")

if __name__ == "__main__":
    predictor = SalaryPredictor()
    
    # Load and prepare data
    df = predictor.load_data()
    
    if len(df) > 0:
        df = predictor.prepare_features(df)
        
        # Train model
        X_test, y_test = predictor.train_model(df)
        
        # Show results
        predictor.show_predictions(X_test, y_test)
        predictor.show_feature_importance()
        
        # Save model
        predictor.save_model()
        
        # Test prediction
        print("\n" + "=" * 60)
        print("🧪 TEST PREDICTION")
        print("=" * 60)
        
        test_salary = predictor.predict_salary(
            job_title="Senior Python Engineer",
            skills=['python', 'aws', 'docker', 'kubernetes'],
            location='Remote - USA',
            experience='Senior'
        )
        
        print(f"\n📊 Job: Senior Python Engineer")
        print(f"   Skills: Python, AWS, Docker, Kubernetes")
        print(f"   Location: Remote - USA")
        print(f"   💰 Predicted Salary: ${test_salary:,.0f}")
        
        print("\n" + "=" * 60)
    else:
        print("\n⚠️  No salary data available for training")
        print("💡 Scrape more jobs to build the model!")