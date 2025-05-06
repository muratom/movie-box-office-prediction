
from flask import Flask, request, jsonify
import numpy as np
import pandas as pd
import dill
import tensorflow as tf
from keras.models import load_model
from custom_layers import IndexAndExpandLayer
from flask_cors import CORS
import db

# Load the trained Keras model
model = load_model('best_model.keras', custom_objects={"IndexAndExpandLayer": IndexAndExpandLayer})

# Initialize Flask app
app = Flask(__name__)

# Enable CORS for the app
CORS(app)

@app.route('/api/domestic_distributor')
def get_distributors():
    distributors = ["Warner Bros.", "Universal Studios", "Paramount Pictures"]
    return jsonify(distributors)

@app.route('/api/mpaa')
def get_mpaa():
    mpaa_ratings = ["G", "PG", "PG-13", "R", "NC-17"]
    return jsonify(mpaa_ratings)

@app.route('/api/directors')
def get_directors():
    directors = ["Steven Spielberg", "Christopher Nolan", "Quentin Tarantino"]
    return jsonify(directors)

@app.route('/api/production_companies')
def get_production_companies():
    companies = db.PRODUCTION_COMPANIES
    return jsonify(companies)

@app.route('/api/production_countries')
def get_production_countries():
    countries = ["US", "UK", "FR", "DE"]
    return jsonify(countries)

@app.route('/api/actors')
def get_actors():
    actors = ["Tom Hanks", "Leonardo DiCaprio", "Morgan Freeman", "Robert Downey Jr."]
    return jsonify(actors)

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get JSON data from the request
        input_data = request.json

        print(input_data)

        actors = set([input_data[f'actor_{i}'] for i in range(1, 4)])
        if (len(actors) != 3):
            return jsonify({'error': 'All actors must be different'}), 400
        budget = input_data['budget']
        if (budget <= 0):
            return jsonify({'error': 'Budget must be positive'}), 400
        release_month = input_data['release_month']
        if (not (1 <= release_month <= 12)):
            return jsonify({'error': 'Release month must be between 1 and 12'}), 400

        vote_average = input_data.get('vote_average', 0)
        domestic_opening = input_data.get('domestic_opening', 0)
        if ((vote_average is None) ^ (domestic_opening is None)):
            return jsonify({'error': 'Fill in all fields in \'Post-released information\' block'}), 400

        df = pd.DataFrame(input_data)

        with open('./pipeline.pkl', 'rb') as f:
            pipeline = dill.load(f)

        X = pipeline.transform(df)

        Xt = X.values.astype(float)

        prediction = int(model.predict(Xt).argmax(axis=1)[0])
        # print('Prediction:', m.predict([Xt, Xp]).argmax(axis=1))
        print('Prediction:', prediction)

        # Prepare JSON response with predictions
        predicted_range = get_revenue_range(prediction, budget)
        output = {'from': predicted_range[0], 'to': predicted_range[1]}
        return jsonify(output)

    except Exception as e:
        return jsonify({'error': str(e)}), 400

def get_revenue_range(pred, budget):
    if pred == 0:
        return (0, 5000000)
    elif pred == 1:
        return (5000000, 20000000)
    elif pred == 2:
        return (20000000, 60000000)
    elif pred == 3:
        return (60000000, 145000000)
    elif pred == 4:
        return (145000000, -1)
    raise Exception("unkown predicted class")

if __name__ == '__main__':
    # Run the Flask app
    app.run(host='0.0.0.0', port=5000)
