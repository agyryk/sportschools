#!flask/bin/python
from flask import Flask, jsonify, request
from webapp import db

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello, World!"


@app.route('/pins', methods=['GET'])
def get_pins():
    region = request.args.get('region') or "ncva"
    return jsonify(db.get_pins_by_region(region))

@app.route('/info', methods=['GET'])
def get_basic_club_info():
    region = request.args.get('region') or "ncva"
    club_id = request.args.get('club')
    return jsonify(db.get_basic_club_info(club_id, region))


@app.route('/season', methods=['GET'])
def get_club_season_results():
    region = request.args.get('region') or "ncva"
    club_id = request.args.get('club')
    return jsonify(db.get_club_season_results(club_id, region))

if __name__ == '__main__':
    app.run(debug=True)