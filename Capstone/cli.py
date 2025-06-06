from datetime import datetime
import calendar
import mysql.connector as dbconnect
from mysql.connector import Error

from constants import SCRIPTS


conn = None

try:
    conn = dbconnect.connect(
        host='127.0.0.1',
        user='root',
        password='password'
    )

    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("create database if not exists creditcard_capstone_db")
        print("Database created.")

except Error as e:
    print(f"Error: {e}")

finally:
    if conn and conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL closed.")

def get_zip_code():
    while True:
        zip_code = input("Enter a 5-digit ZIP code: ").strip()
        if zip_code.isdigit() and len(zip_code) == 5:
            return zip_code
        print("Invalid ZIP code. Must be 5 digits.")

def get_month():
    while True:
        month = input("Enter the month (1-12): ").strip()
        if month.isdigit() and 1 <= int(month) <= 12:
            return int(month)
        print("Invalid month. Must be between 1 and 12.")

def get_year():
    current_year = datetime.now().year
    while True:
        year = input("Enter the 4-digit year (e.g., 2025): ").strip()
        if year.isdigit() and 1900 <= int(year) <= current_year:
            return int(year)
        print(f"Invalid year. Must be between 1900 and {current_year}.")

def fetch_transactions(zip_code, month, year):
    try:
        conn = dbconnect.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone_db'
        )

        if conn.is_connected():
            cursor = conn.cursor(dictionary=True)
            query = SCRIPTS['get_transactions']
            cursor.execute(query, (zip_code, month, year))
            results = cursor.fetchall()

            if results:
                for row in results:
                    print(f"{row['FIRST_NAME']} {row['LAST_NAME']} | "
                          f"Type: {row['TRANSACTION_TYPE']} | "
                          f"Amount: ${row['TRANSACTION_VALUE']} | "
                          f"Date: {row['TRANSACTION_MONTH']:02d}/"
                          f"{row['TRANSACTION_DAY']:02d}/"
                          f"{row['TRANSACTION_YEAR']}")
            else:
                print("No transactions found.")

    except Error as e:
        print(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def generate_monthly_statement(cc_no, month, year):
    last_day = calendar.monthrange(year, month)[1]
    try:
        conn = dbconnect.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone_db'
        )

        if conn.is_connected():
            cursor = conn.cursor(dictionary=True)
            
            first_day_of_month = int(f"{year:04d}{month:02d}01")
            prev_balance_query = SCRIPTS['prev_balance']
            cursor.execute(prev_balance_query, (cc_no, first_day_of_month))
            prev_balance = cursor.fetchone()['PREVIOUS_BALANCE']
            
            current_month_query = SCRIPTS['current_transactions']
            cursor.execute(current_month_query, (cc_no, month, year))
            transactions = cursor.fetchall()

            if not transactions:
                print("No transactions found for this card and period.")
                return

            customer_name = f"{transactions[0]['FIRST_NAME']} {transactions[0]['LAST_NAME']}"

            current_month_total = sum(tx['TRANSACTION_VALUE'] for tx in transactions)

            # New balance = previous balance + current month total
            new_balance = prev_balance + current_month_total

            # Minimum payment based on new balance
            min_payment = max(round(new_balance * 0.05, 2), 25) if new_balance > 0 else 0

            last_day = calendar.monthrange(year, month)[1]

            # Print statement
            print("\n" + "="*50)
            print(f"CREDIT CARD STATEMENT - {month}/{year}")
            print("="*50)
            print(f"Customer Name  : {customer_name}")
            print(f"Credit Card No : {cc_no}")
            print(f"Billing Period : {month}/01/{year} to {month}/{last_day}/{year}")
            print(f"Previous Balance: ${prev_balance:.2f}")
            print(f"Current Charges: ${current_month_total:.2f}")
            print(f"New Balance   : ${new_balance:.2f}")
            print(f"Min Payment Due: ${min_payment:.2f}")
            if month != 12:
                print(f"Due Date       : {month+1}/{last_day}/{year}")
            else:
                print(f"Due Date       : 01/{last_day}/{year+1}")
            print("-" * 50)
            print("DATE        | TYPE       | AMOUNT    | BRANCH")
            print("-" * 50)
            for tx in transactions:
                date = datetime.strptime(tx['TIMEID'], '%Y%m%d').strftime('%Y-%m-%d')
                print(f"{date} | {tx['TRANSACTION_TYPE']:10} | ${tx['TRANSACTION_VALUE']:8.2f} | {tx['BRANCH_CODE']}")
            print("="*50)

    except dbconnect.Error as e:
        print(f"Database error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def query_account_details(ssn):
    try:
        conn = dbconnect.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone_db'
        )
        if conn.is_connected():
            cursor = conn.cursor(dictionary=True)
            query = "SELECT * FROM customer_data WHERE SSN = %s"
            cursor.execute(query, (ssn,))
            result = cursor.fetchone()
            if result:
                print("Customer Account Details:")
                for key, value in result.items():
                    print(f"{key}: {value}")
            else:
                print("No customer found with that SSN.")

    except dbconnect.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def modify_account_details(ssn, field, new_value):
    try:
        conn = dbconnect.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone_db'
        )
        if conn.is_connected():
            cursor = conn.cursor()
            query = f"UPDATE customer_data SET {field} = %s WHERE SSN = %s"
            cursor.execute(query, (new_value, ssn))
            conn.commit()
            print("Account updated successfully.")

    except dbconnect.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def query_transactions_between_dates(start_date, end_date):
    try:
        conn = dbconnect.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone_db'
        )
        if conn.is_connected():
            cursor = conn.cursor(dictionary=True)
            query = SCRIPTS['timeframe_transactions']
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            for row in results:
                print(f"{row['CUST_CC_NO']} | Type: {row['TRANSACTION_TYPE']} | "
                      f"Amount: ${row['TRANSACTION_VALUE']} | Date: {row['TIMEID']}")
    except dbconnect.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def main_menu():
    while True:
        print("\n[ Main Menu ]")
        print("0. Exit")
        print("1. View transactions by ZIP/month/year")
        print("2. Generate monthly bill")
        print("3. Search account details")
        print("4. Modify account details")
        print("5. Lookup transactions between two dates")
        
        choice = input("Choose an option (0-5): ").strip()

        match choice:
            case '1':
                zip_code = get_zip_code()
                month = get_month()
                year = get_year()
                fetch_transactions(zip_code, month, year)

            case '2':
                cc_no = input("Enter Credit Card Number: ").strip()
                month = get_month()
                year = get_year()
                generate_monthly_statement(cc_no, month, year)

            case '3':
                ssn = input("Enter Customer SSN: ").strip()
                query_account_details(ssn)

            case '4':
                ssn = input("Enter Customer SSN: ").strip()
                field = input("Enter field name to update (e.g., EMAIL, CUST_PHONE): ").strip()
                new_value = input("Enter new value: ").strip()
                modify_account_details(ssn, field, new_value)

            case '5':
                start_date = input("Enter start date (YYYYMMDD): ").strip()
                end_date = input("Enter end date (YYYYMMDD): ").strip()
                query_transactions_between_dates(start_date, end_date)

            case '0':
                print("Goodbye.")
                break

            case _:
                print("Invalid option. Try again.")
