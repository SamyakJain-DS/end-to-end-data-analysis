from flask import Flask, jsonify, request
from mysqldatabase import Database

dbo = Database()

app = Flask(__name__)

@app.route('/')
def index():
    return "API Dictionary"

@app.route('/column', methods=['GET'])
def fetch_data():
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

@app.route('/prep-smartphones')
def prep_smartphones():
    data = dbo.execute_query("""
            SELECT cpu_cores + cpu_speed as 'cpu', 5g, nfc, ir_blaster, ram, storage,
            battery, screen_size, refresh_rate, ppi, rear_cameras,
            rear_primary, front_cameras, front_primary, expandable_upto, spec_score
            FROM mobiles
             """
    ).to_dict(orient='records')
    return jsonify(data)

@app.route('/prep-tablets')
def prep_tablets():
    data = dbo.execute_query("""
            SELECT cpu_cores + cpu_speed as 'cpu', has_sim, has_5G, has_nfc,
            has_irblaster, ram, inbuilt_storage, battery_capacity,
            fast_charging, screen_size, screen_refresh_rate, ppi,
            rear_cameras, rear_primary, front_cameras, front_primary,
            expandable, spec_score
            FROM tablets
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

@app.route('/prep-smartphones-brand', methods=['GET'])
def prep_smartphones_brand():
    brand = request.args.get('brand', default=None, type=str)
    data = dbo.execute_query(f"""
            SELECT cpu_cores + cpu_speed as 'cpu', 5g, nfc, ir_blaster, ram, storage,
            battery, screen_size, refresh_rate, ppi, rear_cameras,
            rear_primary, front_cameras, front_primary, expandable_upto,
            user_rating, price, spec_score
            FROM mobiles
            WHERE brand = '{brand}'
             """
    ).to_dict(orient='records')
    return jsonify(data)

@app.route('/prep-tablets-brand', methods=['GET'])
def prep_tablets_brand():
    brand = request.args.get('brand', default=None, type=str)
    data = dbo.execute_query(f"""
            SELECT cpu_cores + cpu_speed as 'cpu', has_sim, has_5G, has_nfc,
            has_irblaster, ram, inbuilt_storage, battery_capacity,
            fast_charging, screen_size, screen_refresh_rate, ppi,
            rear_cameras, rear_primary, front_cameras, front_primary,
            expandable, user_rating, price, spec_score
            FROM tablets
            WHERE brand = '{brand}'
             """
    ).to_dict(orient='records')
    return jsonify(data)

app.run(debug=True)
dbo.close()