# 🧮 Database Management

To support robust querying and efficient analytics, we implemented both **OLTP** and **OLAP** database systems.

## 🏢 OLTP (Online Transaction Processing)
- Used **MySQL** hosted on **Aiven** for high availability, security, and ACID compliance
- Stored both raw and cleaned data in the online hosted database service **Aiven**.

## 🧠 OLAP (Online Analytical Processing)
- Designed a data warehouse (also on Aiven) for complex, multi-dimensional analytics
- Performed ETL (Extract, Transform, Load) to move structured data from OLTP to OLAP
- Enabled fast aggregation queries — e.g., average price by brand, spec trends, category comparisons — without disrupting transactional load

## 🔑 Key Steps
- ✅ Set up MySQL on Aiven for secure, managed storage
- ✅ ETLed into a denormalized OLAP schema for analytics
- ✅ Configured automatic backups and access control

> 💡 By separating OLTP and OLAP responsibilities, we ensured performance, scalability, and analytical flexibility for all components downstream.

