# 🏦 Per Scholas Capstone Project

This project was developed as part of the **Per Scholas Data Engineering** program. The goal was to build a robust ETL data pipeline for a financial institution using **PySpark**, **MySQL**, and a **Python-based CLI**. The pipeline extracts data from multiple sources, transforms it for quality and consistency, loads it into a relational database, and includes business intelligence visualizations.

---

## 📁 Project Structure

| File               | Description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| `pipeline.py`       | Performs the ETL process on local JSON files and data from the Loan API     |
| `cli.py`           | Command-line interface to query data by ZIP code, SSN, or date range         |
| `visualizations.py`| Generates charts using Matplotlib and Seaborn to visualize key metrics       |

---

## 🧰 Tech Stack

| Tool                    | Purpose                                        |
|-------------------------|-----------------------------------------------|
| **Python 3**            | Core programming language                      |
| **Apache Spark (PySpark)** | Large-scale data processing and transformation |
| **MySQL**               | Relational database for storing transformed data |

---

## 🔧 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/ATrubyArena/Per-Scholas.git
cd Per-Scholas/Capstone
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
# For Windows
venv\Scripts\activate
# For macOS/Linux
source venv/bin/activate
```

### 3. Install Required Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the Application

```bash
python pipeline.py
```

---

## 🤝 Contributions

Contributions are welcome! If you'd like to suggest improvements, fix bugs, or expand features:

1. Fork the repository  
2. Create a new branch (`git checkout -b feature-name`)  
3. Commit your changes (`git commit -m 'Add new feature'`)  
4. Push to the branch (`git push origin feature-name`)  
5. Create a pull request

---

## 📬 Contact

For questions, feedback, or collaboration opportunities:

- **Author:** Alexander Truby-Arena  
- **GitHub:** [ATrubyArena](https://github.com/ATrubyArena)  
- **Email:** [alerxander.trubyarena@gmail.com](alerxander.trubyarena@gmail.com)

---
