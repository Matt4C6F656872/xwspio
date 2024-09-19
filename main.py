# main.py
import logging
from sqlalchemy.orm import sessionmaker
from database import init_db
from parser import process_reports
from queries import execute_query, list_queries, confirm_exit
import sys

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("xwspio.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def update_reports(session):
    print("\n--- Update Reports ---")
    print("Enter espionage report URLs (one per line). Enter 'DONE' when finished:")
    input_urls = []
    while True:
        url = input()
        if url.strip().upper() == 'DONE':
            break
        if url.strip():
            input_urls.append(url.strip())
    if input_urls:
        process_reports(input_urls, session)
    else:
        print("No URLs entered.")

def perform_queries(session):
    print("\n--- Query Menu ---")
    execute_query(session)

def main_menu():
    print("\n=== Espionage Report Manager ===")
    print("1. Update Reports")
    print("2. Query")
    print("3. Exit")

def main():
    setup_logging()

    # Initialize the database and create a session
    engine = init_db()
    Session = sessionmaker(bind=engine)
    session = Session()

    while True:
        main_menu()
        choice = input("Select an option (1-3): ").strip().lower()
        if choice == '1':
            update_reports(session)
        elif choice == '2':
            perform_queries(session)
        elif choice in ['3', 'e', 'exit']:
            confirm_exit()
            # If exit is confirmed, sys.exit is called within confirm_exit
        else:
            print("Invalid choice. Please select a valid option.")

    # Close the session (unreachable code if sys.exit is called)
    session.close()

if __name__ == "__main__":
    main()
