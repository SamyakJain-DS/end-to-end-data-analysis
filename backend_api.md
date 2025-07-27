# 🔌 Backend APIs (Flask)

To power the front-end, we developed modular **RESTful APIs** using **Flask**, which serve processed data on demand.

## 🔧 How It Works
- Exposed endpoints like `/data?category=laptops` that the Streamlit app queries
- APIs connect to the OLAP warehouse and return lightweight, preprocessed results in **JSON** format
- Structured the Flask code using **Blueprints** for maintainability
- Served the app in production using **Gunicorn**, a robust WSGI server

## 🛠️ Implementation Details
- Flask app in Python
- Gunicorn server for concurrency
- CORS enabled to handle frontend-backend communication securely

✅ **Outcome**: A scalable, API-first backend that delivers real-time analytics to the Streamlit dashboard smoothly and securely.

## 📂 Relevant Files <br>
[Flask API Backend Code](./python_files/backend.py)
