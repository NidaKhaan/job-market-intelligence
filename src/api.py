from flask import Flask, jsonify, request
from flask_cors import CORS
from database import Database
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend access

db = Database()

@app.route('/')
def home():
    return jsonify({
        'message': 'Job Market Intelligence API',
        'version': '1.0',
        'endpoints': {
            '/api/jobs': 'Get all jobs',
            '/api/jobs/<id>': 'Get specific job',
            '/api/stats': 'Get statistics',
            '/api/sources': 'Get jobs by source',
            '/api/recent': 'Get recent jobs',
            '/api/search': 'Search jobs (params: q, location, company)',
            '/api/logs': 'Get scraping logs'
        }
    })

@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    """Get all active jobs"""
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT job_id, title, company, location, salary_min, salary_max, 
               requirements, url, source, posted_date
        FROM jobs 
        WHERE is_active = TRUE
        ORDER BY scraped_at DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))
    
    jobs = []
    for row in cursor.fetchall():
        jobs.append({
            'job_id': row[0],
            'title': row[1],
            'company': row[2],
            'location': row[3],
            'salary_min': float(row[4]) if row[4] else None,
            'salary_max': float(row[5]) if row[5] else None,
            'requirements': row[6],
            'url': row[7],
            'source': row[8],
            'posted_date': row[9].isoformat() if row[9] else None
        })
    
    cursor.close()
    db.return_connection(conn)
    
    return jsonify({
        'count': len(jobs),
        'jobs': jobs
    })

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Total jobs
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE is_active = TRUE")
    total_jobs = cursor.fetchone()[0]
    
    # By source
    cursor.execute("""
        SELECT source, COUNT(*) 
        FROM jobs 
        WHERE is_active = TRUE
        GROUP BY source
    """)
    by_source = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Top locations
    cursor.execute("""
        SELECT location, COUNT(*) as cnt
        FROM jobs 
        WHERE is_active = TRUE
        GROUP BY location
        ORDER BY cnt DESC
        LIMIT 10
    """)
    top_locations = [{'location': row[0], 'count': row[1]} for row in cursor.fetchall()]
    
    # Salary stats
    cursor.execute("""
        SELECT 
            AVG(salary_min) as avg_min,
            AVG(salary_max) as avg_max,
            COUNT(*) as with_salary
        FROM jobs 
        WHERE is_active = TRUE AND salary_min IS NOT NULL
    """)
    salary_row = cursor.fetchone()
    salary_stats = {
        'average_min': round(float(salary_row[0]), 2) if salary_row[0] else None,
        'average_max': round(float(salary_row[1]), 2) if salary_row[1] else None,
        'jobs_with_salary': salary_row[2]
    }
    
    cursor.close()
    db.return_connection(conn)
    
    return jsonify({
        'total_jobs': total_jobs,
        'by_source': by_source,
        'top_locations': top_locations,
        'salary_stats': salary_stats,
        'generated_at': datetime.now().isoformat()
    })

@app.route('/api/recent', methods=['GET'])
def get_recent():
    """Get jobs from last 24 hours"""
    hours = request.args.get('hours', 24, type=int)
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT job_id, title, company, location, source, scraped_at
        FROM jobs 
        WHERE is_active = TRUE 
        AND scraped_at >= NOW() - INTERVAL '%s hours'
        ORDER BY scraped_at DESC
    """, (hours,))
    
    jobs = []
    for row in cursor.fetchall():
        jobs.append({
            'job_id': row[0],
            'title': row[1],
            'company': row[2],
            'location': row[3],
            'source': row[4],
            'scraped_at': row[5].isoformat()
        })
    
    cursor.close()
    db.return_connection(conn)
    
    return jsonify({
        'count': len(jobs),
        'hours': hours,
        'jobs': jobs
    })

@app.route('/api/search', methods=['GET'])
def search_jobs():
    """Search jobs by keyword, location, or company"""
    query = request.args.get('q', '')
    location = request.args.get('location', '')
    company = request.args.get('company', '')
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    sql = """
        SELECT job_id, title, company, location, url, source
        FROM jobs 
        WHERE is_active = TRUE
    """
    params = []
    
    if query:
        sql += " AND (title ILIKE %s OR requirements ILIKE %s)"
        params.extend([f'%{query}%', f'%{query}%'])
    
    if location:
        sql += " AND location ILIKE %s"
        params.append(f'%{location}%')
    
    if company:
        sql += " AND company ILIKE %s"
        params.append(f'%{company}%')
    
    sql += " ORDER BY scraped_at DESC LIMIT 50"
    
    cursor.execute(sql, params)
    
    jobs = []
    for row in cursor.fetchall():
        jobs.append({
            'job_id': row[0],
            'title': row[1],
            'company': row[2],
            'location': row[3],
            'url': row[4],
            'source': row[5]
        })
    
    cursor.close()
    db.return_connection(conn)
    
    return jsonify({
        'count': len(jobs),
        'search_params': {
            'query': query,
            'location': location,
            'company': company
        },
        'jobs': jobs
    })

@app.route('/api/logs', methods=['GET'])
def get_logs():
    """Get scraping logs"""
    limit = request.args.get('limit', 20, type=int)
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT run_time, success, jobs_scraped, error_message
        FROM scraping_logs
        ORDER BY run_time DESC
        LIMIT %s
    """, (limit,))
    
    logs = []
    for row in cursor.fetchall():
        logs.append({
            'run_time': row[0].isoformat(),
            'success': row[1],
            'jobs_scraped': row[2],
            'error_message': row[3]
        })
    
    cursor.close()
    db.return_connection(conn)
    
    return jsonify({
        'count': len(logs),
        'logs': logs
    })

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 JOB MARKET INTELLIGENCE API")
    print("=" * 60)
    print("📡 Starting server on http://localhost:5000")
    print("📚 API Documentation: http://localhost:5000")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)