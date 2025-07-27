# End-to-End Electronics Market Analysis Platform

This project simulates a complete, professional data science workflow, from initial data acquisition to the deployment of a live, interactive web application. The platform gathers data on electronic devices (laptops and smartphones), processes it through a robust data engineering pipeline, and allows users to perform custom market analysis through a user-friendly interface.[1] It showcases a full-stack skill set encompassing data engineering, data analysis, API development, and cloud deployment.

## Live Demo & System Architecture

* **Live Application:** <a href="https://samyak-jain-analysis-project.streamlit.app/" target="_blank" rel="noopener noreferrer">https://samyak-jain-analysis-project.streamlit.app</a>

## Architectural Overview

The platform is built on a modern, decoupled, three-tier architecture. This design choice enhances scalability, maintainability, and reusability by separating concerns. The API is not tied to a specific front end; it functions as a standalone analytics service that could serve other clients in the future (e.g., a mobile app or another dashboard).

1.  **Data Layer:** A managed MySQL database hosted on Aiven, logically partitioned into an OLTP schema for data ingestion and an OLAP schema for analytics.
2.  **Service/Logic Layer:** A Python Flask REST API deployed on Render. This service queries the OLAP data warehouse and exposes a series of analytical endpoints.
3.  **Presentation Layer:** An interactive Streamlit web application deployed on Streamlit Cloud. This front-end client consumes data from the Flask API to provide visualizations and user controls.

## Technology Stack & Architecture

The following table provides a comprehensive overview of the technologies used and their roles within the project's architecture. I have separated the documentation of each step for better clarity. Please refer to the  `Component` hyperlinks for each step's documentation.

| Component | Technology | Role & Purpose |
| :--- | :--- | :--- |
| [**Data Acquisition**](./data_gathering.md) | `Python (Selenium, BeautifulSoup4)` | Automated web scraping of dynamic and static e-commerce sites to gather product data (specifications, pricing, reviews).[1] | 
| [**Data Processing**](./data_cleaning_preprocessing.md) | `Python (Pandas)` | Cleaning, transformation, and validation of raw scraped data. Feature engineering to derive analytical variables. |
| **Data Storage** | `MySQL (on Aiven)` | Deployed an OLTP (Online Transaction Processing) database and an OLAP (Online Analytical Processing) warehouse to separate transactional loads from analytics.[1] |
| **Backend API** | `Python (Flask)` | Development of a RESTful API with endpoints for market overview, brand-specific analysis, and price-based filtering.[1] |
| **Frontend UI** | `Streamlit` | Rapid development of a user-friendly, interactive web application for data visualization and insight generation. |
| **Deployment** | `Aiven, Render, Streamlit Cloud` | Multi-platform cloud deployment: Managed database on Aiven, containerized API service on Render (PaaS), and application hosting on Streamlit Cloud.[1] |
| **Development** | `Git, Conda` | Version control for collaborative and iterative development. Isolated Python environments for dependency management and reproducibility.[1] |
| **Operations** | `Python (logging module)` | Implementation of structured logging for monitoring application health, debugging errors, and tracking data pipeline execution.[1] |

---

## 🛠️ Local Setup & Replication

To run this project locally, follow these steps:

1. **Clone the Repository**

   `bash`
   ```
   git clone https://github.com/SamyakJain-DS/end-to-end-data-analysis.git
   cd end-to-end-data-analysis
   ```
2. **Create and activate a virtual environment**

   `bash`
   ```
   conda create -n device-analysis python=3.10
   conda activate device-analysis
   ```

3. **Install the dependencies**

   `bash`
   ```
   pip install -r requirements.txt
   ```

5. **You're All Set!**

---

## ✅ Skills Demonstrated

- Advanced Web Scraping (anti-bot bypassing)
- Data Cleaning & Feature Engineering
- Database Design (OLTP/OLAP)
- Flask API Development
- Streamlit Dashboard Creation
- Cloud Deployment & CI/CD
- Modular Codebase & Logging
- End-to-End Project Ownership

---

## 📫 Contact

📧 [Samyak Jain](mailto:samyakjain2411@gmail.com)  
🔗 [LinkedIn](linkedin.com/in/samyakjain-ds/)
