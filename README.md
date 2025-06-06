# Per Scholas Capstone Project

This project was developed as part of the Per Scholas Data Engineering program. The goal was to build a complete ETL data pipeline for a financial institution using **PySpark**, **MySQL**, and **Python CLI tools**. The pipeline includes data extraction, transformation, loading into a relational database, and visualization of key business metrics.

---

## 📁 Project Structure

- `extract()`: Reads data from local JSON files and a remote API.
- `transform()`: Cleans and standardizes customer, credit, and branch data using PySpark.
- `load()`: Loads transformed data into a MySQL relational database.
- `cli.py`: Command Line Interface to interact with the data (e.g., query by ZIP, SSN, or date range).
- `visualizations.py`: Generates charts using Matplotlib and Seaborn.

---

## 🔧 Installation

```bash
# Clone the repository
git clone https://github.com/ATrubyArena/Per-Scholas.git
cd Per-Scholas/Capstone
