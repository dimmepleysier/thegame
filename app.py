import os
import pymysql.cursors
from flask import Flask, jsonify, request, render_template
import random

# Initialize the Flask application
app = Flask(__name__)

# --- Database Connection Details ---
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'user': os.environ.get('DB_USER2'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': 'thegame',
    'cursorclass': pymysql.cursors.DictCursor
}   


@app.route('/')
def home():
    # Check for a URL parameter like "/?platform=tv"
    # It will default to 'web' if the parameter is not present.
    platform = request.args.get('platform', 'web')
    
    # Pass the platform variable to the HTML template when rendering
    return render_template('index.html', platform=platform)

# ... (rest of your app routes) ...

# --- API Routes ---

@app.route('/get_question')
def get_question():
    """API endpoint to fetch a new, random question from the database."""
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        seen_ids_str = request.args.get('seen_ids', '')
        seen_ids = [int(id) for id in seen_ids_str.split(',') if id.isdigit()]

        with connection.cursor() as cursor:
            sql_question = "SELECT tmdbid, title, filename FROM questions"
            if seen_ids:
                placeholders = ', '.join(['%s'] * len(seen_ids))
                sql_question += f" WHERE tmdbid NOT IN ({placeholders})"
            sql_question += " ORDER BY RAND() LIMIT 1"
            
            cursor.execute(sql_question, seen_ids)
            question = cursor.fetchone()

            if not question:
                return jsonify({"error": "No more questions available"}), 404

            correct_answer = question['title']
            question_id = question['tmdbid']

            sql_wrong_answers = "SELECT title FROM questions WHERE title != %s ORDER BY RAND() LIMIT 7"
            cursor.execute(sql_wrong_answers, (correct_answer,))
            wrong_answers = [row['title'] for row in cursor.fetchall()]

            all_answers = wrong_answers + [correct_answer]
            random.shuffle(all_answers)

            response = {
                "id": question_id,
                "visual": f"/static/images/{question['filename']}",
                "answers": all_answers,
                "correct_answer": correct_answer
            }
            return jsonify(response)

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify({"error": "A database error occurred"}), 500
    finally:
        if connection:
            connection.close()

@app.route('/get_leaderboard')
def get_leaderboard():
    """API endpoint to fetch the top scores, with a configurable limit."""
    # --- FIX: Get limit from request args, default to 10 if not provided ---
    limit = request.args.get('limit', 10, type=int)
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            # --- FIX: Use the limit variable in the SQL query ---
            sql = "SELECT player_name, score FROM leaderboard ORDER BY score DESC LIMIT %s"
            cursor.execute(sql, (limit,))
            leaderboard = cursor.fetchall()
            return jsonify(leaderboard)
    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Could not fetch leaderboard"}), 500
    finally:
        if connection:
            connection.close()

@app.route('/submit_score', methods=['POST'])
def submit_score():
    """API endpoint to save a player's score to the leaderboard."""
    data = request.get_json()
    player_name = data.get('playerName')
    score = data.get('score')

    if not player_name or score is None:
        return jsonify({"success": False, "error": "Invalid data provided"}), 400

    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            sql = "INSERT INTO leaderboard (player_name, score) VALUES (%s, %s)"
            cursor.execute(sql, (player_name, score))
        connection.commit()
        return jsonify({"success": True})
    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
        return jsonify({"success": False, "error": "Database error occurred while saving"}), 500
    finally:
        if connection:
            connection.close()

# --- Main execution point ---
if __name__ == '__main__':
    app.run(debug=True)
