# 🧮 Database Management

To support robust querying and efficient analytics, we implemented both **OLTP** and **OLAP** database systems.

## 🏢 OLTP (Online Transaction Processing)
- Used **MySQL** hosted on **Aiven** for high availability, security, and ACID compliance
- Stored both raw and cleaned data in the online hosted database service **Aiven**.

## 🧠 OLAP (Online Analytical Processing)
- Created a dedicated **OLAP database** to separate analytical workloads from transactional data
- Replicated already **denormalized, analysis-ready tables** into this database for fast aggregations
- Performed ETL using SQL to copy both raw and cleaned datasets from the default transactional database
- Enabled efficient querying on metrics like average price, spec distribution, and brand comparisons

## 🔑 Key Steps
- ✅ Set up MySQL on Aiven for secure, managed storage
- ✅ Normalized OLTP schema for raw transactional data
- ✅ Replicated denormalized data into OLAP layer via SQL-based ETL
- ✅ Configured automatic backups and access control

> 💡 By separating OLTP and OLAP responsibilities, we ensured performance, scalability, and analytical flexibility for all components downstream.

## 📂 Relevant Files <br>
[SQL Helper Code](./python_files/mysqldatabase.py) <br>
[Creating OLAP](./sql_files/creating_olap.sql)
