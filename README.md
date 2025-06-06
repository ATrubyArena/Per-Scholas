🏦 Per Scholas Capstone Project
This project was developed as part of the Per Scholas Data Engineering program. The objective was to build a robust ETL data pipeline for a financial institution using PySpark, MySQL, and Python CLI tools. The pipeline performs data extraction, transformation, and loading (ETL), and it includes business intelligence visualizations.

📁 Project Structure
extract(): Reads data from local JSON files and a remote API.

transform(): Cleans and standardizes customer, credit, and branch data using PySpark.

load(): Loads the transformed data into a MySQL relational database.

cli.py: Provides a Command Line Interface to interact with the data (e.g., query by ZIP, SSN, or date range).

visualizations.py: Generates charts to represent key metrics using Matplotlib and Seaborn.

🧰 Tech Stack
Tool	Purpose
Python 3	Core programming language
PySpark	Big data processing and transformation
MySQL	Relational database for storing transformed data
mysql-connector-python	Python library for interacting with MySQL
Matplotlib / Seaborn	Data visualization libraries
Pandas	Used alongside PySpark for some data manipulation
Requests	To access external API data
Rich	Enhanced console output formatting
VS Code	Development environment (recommended)

🔧 Installation
bash
Copy
Edit
# Clone the repository
git clone https://github.com/ATrubyArena/Per-Scholas.git
cd Per-Scholas/Capstone

# (Optional) Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
