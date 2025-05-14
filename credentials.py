# credentials.py
from urllib.parse import quote_plus # <--- Add this import

DB_USER = 'root'
DB_PASSWORD = '*******' # Your actual password
DB_HOST = 'localhost'  # Or your MySQL host if it's not local
DB_PORT = '3306' # Or your MySQL port
DB_DATABASE = 'phonepe'

# URL encode the password to handle special characters like '@', '#', etc.
# This is crucial if your password contains such characters.
ENCODED_PASSWORD = quote_plus(DB_PASSWORD) # <--- Add this line

# You might still want DB_PASSWORD available, but for the URL, use ENCODED_PASSWORD.
