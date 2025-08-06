# main.py
import sqlite3
import os
from faker import Faker
from datetime import datetime, timedelta
import random

# --- Configuration ---
DB_FILE = "../agents/database_ingestor/ticketing_platform.db"
NUM_VENDORS = 15
NUM_VENUES = 25
NUM_USERS = 200
NUM_EVENT_CATEGORIES = 10
NUM_EVENTS = 50
MAX_TICKETS_PER_EVENT = 4
MIN_SALES_PER_EVENT = 50
MAX_SALES_PER_EVENT = 300

# Initialize Faker for generating mock data
fake = Faker()


def create_connection(db_file):
    """ Create a database connection to the SQLite database specified by db_file """
    conn = None
    try:
        # Delete the old database file if it exists to start fresh
        if os.path.exists(db_file):
            os.remove(db_file)
            print(f"Removed old database file: {db_file}")

        conn = sqlite3.connect(db_file)
        print(f"SQLite version: {sqlite3.version}")
        print(f"Successfully connected to database: {db_file}")

        # Enable foreign key constraint enforcement
        conn.execute("PRAGMA foreign_keys = ON;")

    except sqlite3.Error as e:
        print(e)
    return conn


def create_tables(conn):
    """ Create tables for the ticketing platform """
    try:
        cursor = conn.cursor()

        # --- Table Creation SQL Statements ---
        # Vendors Table: Organizers of events
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS Vendors
                       (
                           vendor_id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           vendor_name
                           TEXT
                           NOT
                           NULL
                           UNIQUE,
                           contact_email
                           TEXT
                           NOT
                           NULL
                           UNIQUE,
                           contact_phone
                           TEXT,
                           registered_date
                           DATETIME
                           NOT
                           NULL,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP
                       );
                       """)
        print("Table 'Vendors' created successfully.")

        # Venues Table: Locations for events
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS Venues
                       (
                           venue_id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           venue_name
                           TEXT
                           NOT
                           NULL,
                           address
                           TEXT
                           NOT
                           NULL,
                           city
                           TEXT
                           NOT
                           NULL,
                           state
                           TEXT
                           NOT
                           NULL,
                           zip_code
                           TEXT
                           NOT
                           NULL,
                           capacity
                           INTEGER
                           NOT
                           NULL,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP
                       );
                       """)
        print("Table 'Venues' created successfully.")

        # EventCategories Table: To categorize events (e.g., Music, Sports)
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS EventCategories
                       (
                           category_id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           category_name
                           TEXT
                           NOT
                           NULL
                           UNIQUE,
                           description
                           TEXT,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP
                       );
                       """)
        print("Table 'EventCategories' created successfully.")

        # Events Table: The core events
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS Events
                       (
                           event_id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           event_name
                           TEXT
                           NOT
                           NULL,
                           event_date
                           DATETIME
                           NOT
                           NULL,
                           description
                           TEXT,
                           vendor_id
                           INTEGER
                           NOT
                           NULL,
                           venue_id
                           INTEGER
                           NOT
                           NULL,
                           category_id
                           INTEGER
                           NOT
                           NULL,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           FOREIGN
                           KEY
                       (
                           vendor_id
                       ) REFERENCES Vendors
                       (
                           vendor_id
                       ),
                           FOREIGN KEY
                       (
                           venue_id
                       ) REFERENCES Venues
                       (
                           venue_id
                       ),
                           FOREIGN KEY
                       (
                           category_id
                       ) REFERENCES EventCategories
                       (
                           category_id
                       )
                           );
                       """)
        print("Table 'Events' created successfully.")

        # Users Table: Customers buying tickets
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS Users
                       (
                           user_id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           first_name
                           TEXT
                           NOT
                           NULL,
                           last_name
                           TEXT
                           NOT
                           NULL,
                           email
                           TEXT
                           NOT
                           NULL
                           UNIQUE,
                           password_hash
                           TEXT
                           NOT
                           NULL,
                           registration_date
                           DATETIME
                           NOT
                           NULL,
                           last_login
                           DATETIME,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP
                       );
                       """)
        print("Table 'Users' created successfully.")

        # Tickets Table: Types of tickets for each event
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS Tickets
                       (
                           ticket_id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           event_id
                           INTEGER
                           NOT
                           NULL,
                           ticket_type
                           TEXT
                           NOT
                           NULL, -- e.g., 'General Admission', 'VIP', 'Early Bird'
                           price
                           REAL
                           NOT
                           NULL,
                           quantity_available
                           INTEGER
                           NOT
                           NULL,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           FOREIGN
                           KEY
                       (
                           event_id
                       ) REFERENCES Events
                       (
                           event_id
                       )
                           );
                       """)
        print("Table 'Tickets' created successfully.")

        # Sales Table: Transactional data for ticket purchases
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS Sales
                       (
                           sale_id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           user_id
                           INTEGER
                           NOT
                           NULL,
                           event_id
                           INTEGER
                           NOT
                           NULL,
                           ticket_id
                           INTEGER
                           NOT
                           NULL,
                           quantity_purchased
                           INTEGER
                           NOT
                           NULL,
                           total_price
                           REAL
                           NOT
                           NULL,
                           purchase_date
                           DATETIME
                           NOT
                           NULL,
                           transaction_id
                           TEXT
                           NOT
                           NULL
                           UNIQUE,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           FOREIGN
                           KEY
                       (
                           user_id
                       ) REFERENCES Users
                       (
                           user_id
                       ),
                           FOREIGN KEY
                       (
                           event_id
                       ) REFERENCES Events
                       (
                           event_id
                       ),
                           FOREIGN KEY
                       (
                           ticket_id
                       ) REFERENCES Tickets
                       (
                           ticket_id
                       )
                           );
                       """)
        print("Table 'Sales' created successfully.")

        # CheckIns Table: To track ticket usage at events
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS CheckIns
                       (
                           check_in_id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           sale_id
                           INTEGER
                           NOT
                           NULL,
                           event_id
                           INTEGER
                           NOT
                           NULL,
                           user_id
                           INTEGER
                           NOT
                           NULL,
                           check_in_time
                           DATETIME
                           NOT
                           NULL,
                           created_at
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP,
                           FOREIGN
                           KEY
                       (
                           sale_id
                       ) REFERENCES Sales
                       (
                           sale_id
                       ),
                           FOREIGN KEY
                       (
                           event_id
                       ) REFERENCES Events
                       (
                           event_id
                       ),
                           FOREIGN KEY
                       (
                           user_id
                       ) REFERENCES Users
                       (
                           user_id
                       )
                           );
                       """)
        print("Table 'CheckIns' created successfully.")

        conn.commit()

    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")
        conn.rollback()


def populate_data(conn):
    """ Populate tables with realistic mock data """
    try:
        cursor = conn.cursor()

        # --- Populate Vendors ---
        vendors = []
        for _ in range(NUM_VENDORS):
            vendor_name = fake.company() + " " + fake.company_suffix()
            vendors.append((
                vendor_name,
                fake.unique.email(),
                fake.phone_number(),
                fake.date_time_this_decade()
            ))
        cursor.executemany(
            "INSERT INTO Vendors (vendor_name, contact_email, contact_phone, registered_date) VALUES (?, ?, ?, ?)",
            vendors)
        print(f"Populated {cursor.rowcount} records into 'Vendors'.")

        # --- Populate Venues ---
        venues = []
        for _ in range(NUM_VENUES):
            venues.append((
                fake.company() + " Arena",
                fake.street_address(),
                fake.city(),
                fake.state_abbr(),
                fake.zipcode(),
                random.randint(500, 20000)
            ))
        cursor.executemany(
            "INSERT INTO Venues (venue_name, address, city, state, zip_code, capacity) VALUES (?, ?, ?, ?, ?, ?)",
            venues)
        print(f"Populated {cursor.rowcount} records into 'Venues'.")

        # --- Populate Event Categories ---
        categories = [
            ('Music Concert', 'Live music performances by various artists.'),
            ('Sports', 'Sporting events including basketball, football, and more.'),
            ('Theater & Arts', 'Plays, musicals, and other artistic performances.'),
            ('Comedy', 'Stand-up comedy shows and tours.'),
            ('Family & Attractions', 'Events suitable for all ages.'),
            ('Festivals', 'Multi-day music and arts festivals.'),
            ('Conferences', 'Professional and academic conferences.'),
            ('Food & Drink', 'Culinary events, wine tasting, and food festivals.'),
            ('Charity & Gala', 'Fundraising events and formal galas.'),
            ('Exhibitions', 'Art galleries and museum exhibitions.')
        ]
        cursor.executemany("INSERT INTO EventCategories (category_name, description) VALUES (?, ?)", categories)
        print(f"Populated {cursor.rowcount} records into 'EventCategories'.")

        # --- Populate Users ---
        users = []
        for _ in range(NUM_USERS):
            users.append((
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.password(),
                fake.date_time_this_decade(),
                fake.date_time_this_year()
            ))
        cursor.executemany(
            "INSERT INTO Users (first_name, last_name, email, password_hash, registration_date, last_login) VALUES (?, ?, ?, ?, ?, ?)",
            users)
        print(f"Populated {cursor.rowcount} records into 'Users'.")

        # --- Populate Events and Tickets ---
        events = []
        all_tickets = []
        for _ in range(NUM_EVENTS):
            event_date = fake.date_time_between(start_date="+1d", end_date="+1y")
            events.append((
                " ".join(fake.words(nb=3)).title() + " Live",
                event_date,
                fake.paragraph(nb_sentences=3),
                random.randint(1, NUM_VENDORS),
                random.randint(1, NUM_VENUES),
                random.randint(1, NUM_EVENT_CATEGORIES)
            ))
        cursor.executemany(
            "INSERT INTO Events (event_name, event_date, description, vendor_id, venue_id, category_id) VALUES (?, ?, ?, ?, ?, ?)",
            events)
        print(f"Populated {cursor.rowcount} records into 'Events'.")

        # Fetch event_ids to create tickets for them
        cursor.execute("SELECT event_id FROM Events")
        event_ids = [row[0] for row in cursor.fetchall()]

        ticket_types = [
            ('General Admission', 1.0),
            ('VIP', 2.5),
            ('Early Bird', 0.8),
            ('Balcony', 1.2),
            ('Front Row', 3.0)
        ]
        for event_id in event_ids:
            num_ticket_types = random.randint(1, MAX_TICKETS_PER_EVENT)
            selected_types = random.sample(ticket_types, num_ticket_types)
            for ticket_type, price_multiplier in selected_types:
                base_price = random.uniform(25.0, 200.0)
                all_tickets.append((
                    event_id,
                    ticket_type,
                    round(base_price * price_multiplier, 2),
                    random.randint(100, 1000)
                ))
        cursor.executemany("INSERT INTO Tickets (event_id, ticket_type, price, quantity_available) VALUES (?, ?, ?, ?)",
                           all_tickets)
        print(f"Populated {cursor.rowcount} records into 'Tickets'.")

        # --- Populate Sales and CheckIns ---
        sales = []
        checkins = []

        # Fetch all ticket information for creating sales
        cursor.execute("SELECT ticket_id, event_id, price FROM Tickets")
        ticket_data = cursor.fetchall()

        cursor.execute("SELECT event_id, event_date FROM Events")
        event_dates = {row[0]: datetime.fromisoformat(row[1]) for row in cursor.fetchall()}

        # Group tickets by event
        event_tickets = {}
        for ticket_id, event_id, price in ticket_data:
            if event_id not in event_tickets:
                event_tickets[event_id] = []
            event_tickets[event_id].append({'ticket_id': ticket_id, 'price': price})

        sale_id_counter = 1
        for event_id, tickets_for_event in event_tickets.items():
            if not tickets_for_event:
                continue

            num_sales = random.randint(MIN_SALES_PER_EVENT, MAX_SALES_PER_EVENT)
            event_date = event_dates[event_id]

            for _ in range(num_sales):
                user_id = random.randint(1, NUM_USERS)
                ticket_info = random.choice(tickets_for_event)
                ticket_id = ticket_info['ticket_id']
                price = ticket_info['price']
                quantity = random.randint(1, 4)
                total_price = round(price * quantity, 2)
                # Sales happen before the event
                purchase_date = fake.date_time_between(start_date="-6m", end_date=event_date - timedelta(days=1))

                sales.append((
                    user_id,
                    event_id,
                    ticket_id,
                    quantity,
                    total_price,
                    purchase_date,
                    fake.uuid4()
                ))

                # Simulate check-ins for a portion of sales if the event has passed
                if event_date < datetime.now() and random.random() < 0.8:  # 80% check-in rate
                    checkin_time = event_date + timedelta(minutes=random.randint(5, 120))
                    checkins.append((
                        sale_id_counter,  # Corresponds to the sale being inserted
                        event_id,
                        user_id,
                        checkin_time
                    ))
                sale_id_counter += 1

        cursor.executemany(
            "INSERT INTO Sales (user_id, event_id, ticket_id, quantity_purchased, total_price, purchase_date, transaction_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            sales)
        print(f"Populated {cursor.rowcount} records into 'Sales'.")

        cursor.executemany("INSERT INTO CheckIns (sale_id, event_id, user_id, check_in_time) VALUES (?, ?, ?, ?)",
                           checkins)
        print(f"Populated {cursor.rowcount} records into 'CheckIns'.")

        conn.commit()

    except sqlite3.Error as e:
        print(f"Error populating data: {e}")
        conn.rollback()


def main():
    """ Main function to orchestrate DB creation and population """
    conn = create_connection(DB_FILE)

    if conn is not None:
        create_tables(conn)
        populate_data(conn)
        conn.close()
        print("Database script finished successfully.")
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    main()
