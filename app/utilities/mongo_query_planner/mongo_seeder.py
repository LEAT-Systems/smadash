import random
from faker import Faker
from pymongo.mongo_client import MongoClient
import os
from dotenv import load_dotenv

# --- CONFIG ---

# Load variables from .env file into environment
load_dotenv()

# Access them like normal environment variables
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "ticketing_platform"

fake = Faker()
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# --- CLEAR OLD DATA (safe re-run) ---
for collection in ["Users", "Vendors", "Venues", "Events", "Tickets", "Sales"]:
    db[collection].delete_many({})

# --- INSERT DUMMY DATA ---

# Users
users = [{
    "first_name": fake.first_name(),
    "last_name": fake.last_name(),
    "email": fake.unique.email(),
    "phone": fake.phone_number()
} for _ in range(20)]
user_ids = db.Users.insert_many(users).inserted_ids

# Vendors
vendors = [{
    "vendor_name": fake.company(),
    "contact_email": fake.company_email()
} for _ in range(5)]
vendor_ids = db.Vendors.insert_many(vendors).inserted_ids

# Venues
venues = [{
    "venue_name": fake.company() + " Hall",
    "address": fake.street_address(),
    "city": fake.city(),
    "state": fake.state(),
    "zip_code": fake.zipcode()
} for _ in range(5)]
venue_ids = db.Venues.insert_many(venues).inserted_ids

# Events
events = [{
    "title": fake.catch_phrase(),
    "date": fake.date_time_between(start_date="+1d", end_date="+6M"),
    "venue_id": random.choice(venue_ids),
    "vendor_id": random.choice(vendor_ids)
} for _ in range(10)]
event_ids = db.Events.insert_many(events).inserted_ids

# Tickets
tickets = [{
    "event_id": random.choice(event_ids),
    "price": round(random.uniform(20, 150), 2),
    "seat_number": f"{random.randint(1, 30)}-{random.choice('ABCDE')}"
} for _ in range(30)]
ticket_ids = db.Tickets.insert_many(tickets).inserted_ids

# Sales
sales = [{
    "user_id": random.choice(user_ids),
    "ticket_id": random.choice(ticket_ids),
    "purchase_date": fake.date_time_between(start_date="-3M", end_date="now")
} for _ in range(25)]
db.Sales.insert_many(sales)

print("\n✅ Database populated successfully!\n")

# --- VERIFICATION ---
print("Sample documents:\n")
print("Users:", list(db.Users.find().limit(2)))
print("\nVendors:", list(db.Vendors.find().limit(2)))
print("\nVenues:", list(db.Venues.find().limit(2)))
print("\nEvents:", list(db.Events.find().limit(2)))
print("\nTickets:", list(db.Tickets.find().limit(2)))
print("\nSales:", list(db.Sales.find().limit(2)))
