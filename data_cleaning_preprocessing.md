# 🧹 Data Cleaning & Preprocessing

The raw scraped data was highly unstructured.

To transform it into an analysis-ready format, we followed a multi-step cleaning pipeline using `Pandas` and custom Python scripts:

## 🧰 Cleaning Steps
- Extracted relevant fields
- Converted price and spec strings into standard numeric formats
- Normalized units and formats (e.g., storage sizes, battery ratings)
- Dropped duplicate entries
- Imputed or removed missing/null values
- Used regular expressions to parse messy text fields

> 🧠 "Garbage In, Garbage Out" — we followed this data science principle to ensure high-quality input for downstream tasks.

## 📤 Output
- Exported cleaned data to `.csv` formats
- Used these files to populate OLTP and OLAP databases

✅ **Result**: A clean, consistent dataset — ready for reliable analysis, EDA, and modeling.

## 📂 Relevant Files <br>
[Cleaning Laptops Dataset](./python_files/cleaning_laptops_df.ipynb) <br>
[Cleaning Smartphohes Dataset](./python_files/cleaning_mobiles_df.ipynb) <br>
[Cleaning Tablets Dataset](./python_files/cleaning_tablets_df.ipynb) <br>
