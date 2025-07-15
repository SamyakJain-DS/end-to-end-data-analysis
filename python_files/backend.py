from flask import Flask, jsonify, request
from mysqldatabase import Database

dbo = Database()

app = Flask(__name__)

@app.route('/')
def index():
    return "API Dictionary"

@app.route('/column', methods=['GET'])
def top_n_brands():
    category = request.args.get('category', default=None, type=str)
    col = request.args.get('col', default=None, type=str)

    data = dbo.execute_query(
        f'''
        SELECT {col}
        FROM {category}
        '''
    ).to_dict(orient='records')
    return jsonify(data)

@app.route('/prep-laptop')
def prep_laptop():
    data = dbo.execute_query("""
            SELECT gpu_vram, ram_capacity, hdd, ssd, ppi, touchscreen, cpu_cores+cpu_threads as 'cpu', spec_score
            FROM laptops
             """
    ).to_dict(orient='records')
    return jsonify(data)

@app.route('/prep-laptop-brand', methods=['GET'])
def prep_laptop_brand():
    brand = request.args.get('brand', default=None, type=str)
    data = dbo.execute_query(f"""
            SELECT gpu_vram, ram_capacity, hdd, ssd, ppi, touchscreen, cpu_cores+cpu_threads as 'cpu', spec_score
            FROM laptops
            WHERE brand = '{brand}'
             """
    ).to_dict(orient='records')
    return jsonify(data)

app.run(debug=True)
dbo.close()

