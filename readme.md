End-to-End Electronics Market Analysis Platform
This project simulates a complete, professional data science workflow, from initial data acquisition to the deployment of a live, interactive web application. The platform gathers data on electronic devices (laptops and smartphones), processes it through a robust data engineering pipeline, and allows users to perform custom market analysis through a user-friendly interface. It showcases a full-stack skill set encompassing data engineering, data analysis, API development, and cloud deployment.   

Live Demo & System Architecture
Live Application: ``

Live API Documentation: ``

Architectural Overview
The platform is built on a modern, decoupled, three-tier architecture. This design choice enhances scalability, maintainability, and reusability by separating concerns. The API is not tied to a specific front end; it functions as a standalone analytics service that could serve other clients in the future (e.g., a mobile app or another dashboard).

Data Layer: A managed PostgreSQL database hosted on Aiven, logically partitioned into an OLTP schema for data ingestion and an OLAP schema for analytics.

Service/Logic Layer: A Python Flask REST API deployed on Render. This service queries the OLAP data warehouse and exposes a series of analytical endpoints.

Presentation Layer: An interactive Streamlit web application deployed on Streamlit Cloud. This front-end client consumes data from the Flask API to provide visualizations and user controls.

Technology Stack & Architecture
The following table provides a comprehensive overview of the technologies used and their roles within the project's architecture.

Component	Technology	Role & Purpose
Data Acquisition	Python (Selenium, BeautifulSoup4)	
Automated web scraping of dynamic and static e-commerce sites to gather product data (specifications, pricing, reviews).   

Data Processing	Python (Pandas)	Cleaning, transformation, and validation of raw scraped data. Feature engineering to derive analytical variables.
Data Storage	PostgreSQL (on Aiven)	
OLTP Schema: Staging area for normalized, cleaned transactional data. OLAP Schema: Data warehouse with denormalized, aggregated tables optimized for complex analytical queries.   

Backend API	Python (Flask)	
Development of a RESTful API with endpoints for market overview, brand-specific analysis, and price-based filtering.   

Frontend UI	Streamlit	Rapid development of a user-friendly, interactive web application for data visualization and insight generation.
Deployment	Aiven, Render, Streamlit Cloud	
Multi-platform cloud deployment: Managed database on Aiven, containerized API service on Render (PaaS), and application hosting on Streamlit Cloud.   

Development	Git, Conda	
Version control for collaborative and iterative development. Isolated Python environments for dependency management and reproducibility.   

Operations	Python (logging module)	
Implementation of structured logging for monitoring application health, debugging errors, and tracking data pipeline execution.   

