import requests
import time
import json
import schedule
import matplotlib.pyplot as plt
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import urllib3

# Disable SSL warnings if using verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Function to fetch Bitcoin price from CoinDesk API
def fetch_bitcoin_price():
    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    print("Fetching Bitcoin price from API...")
    response = requests.get(url, verify=False)  # Set verify=False if SSL issues persist
    data = response.json()
    price_usd = data['bpi']['USD']['rate_float']
    print(f"Fetched Bitcoin price: {price_usd} USD")
    return price_usd


# Function to save/update the Bitcoin price in a JSON file
def update_price_in_file(price, filename='bitcoin_price_cycle.json'):
    print(f"Updating Bitcoin price in file: {filename}...")
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    entry = {'time': current_time, 'price': price}

    try:
        with open(filename, 'r') as file:
            prices = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        prices = []

    prices.append(entry)

    with open(filename, 'w') as file:
        json.dump(prices, file, indent=4)

    print("Price updated in file.")


# Function to generate a graph of the Bitcoin price for the current cycle
def generate_graph(filename='bitcoin_price_cycle.json'):
    print("Generating graph of Bitcoin prices...")

    with open(filename, 'r') as file:
        data = json.load(file)

    times = [entry['time'] for entry in data]
    prices = [entry['price'] for entry in data]

    current_time = datetime.now()
    graph_filename = current_time.strftime('bitcoin_price_graph_%Y-%m-%d_%H-%M-%S.png')

    plt.figure(figsize=(10, 5))
    plt.plot(times, prices, label='Bitcoin Price (USD)')
    plt.xlabel('Time')
    plt.ylabel('Price (USD)')
    plt.title('Bitcoin Price Index (BPI) - Last 10 Minutes')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.legend()
    plt.savefig(graph_filename)
    plt.close()  # Close the figure to avoid display issues

    print(f"Graph generated and saved as '{graph_filename}'.")
    return graph_filename


# Function to send an email with the maximum price in the last cycle
def send_email(max_price, graph_filename):
    print("Sending email with the maximum Bitcoin price...")
    sender_email = 'emil.pinhasov@gmail.com'
    receiver_email = 'emil.pinhasov@gmail.com'
    password = 'rfym bbdk pevx atnk'

    subject = "Bitcoin Price Alert"
    body = f"The maximum Bitcoin price in the last hour was: ${max_price:.2f} USD\nSee the attached graph for details."

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    with open(graph_filename, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())

    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {graph_filename}",
    )

    msg.attach(part)

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(sender_email, password)
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)

    print("Email sent successfully.")


# Function to calculate and send the max price in the last cycle
def handle_max_price(filename='bitcoin_price_cycle.json'):

    with open(filename, 'r') as file:
        data = json.load(file)

    prices = [entry['price'] for entry in data]
    max_price = max(prices)
    print(f"Maximum price found: {max_price} USD")

    graph_filename = generate_graph(filename)
    send_email(max_price, graph_filename)


# Schedule the tasks
schedule.every(1).minutes.do(lambda: update_price_in_file(fetch_bitcoin_price()))
print("Scheduled task to fetch and update Bitcoin price every minute.")

# Run this task every 10 minutes to generate a graph and send an email
schedule.every(1).hours.do(lambda: handle_max_price())
print("Scheduled task to generate a graph and send email .")

# Run the scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(30)
    print("Waiting for the next scheduled task...")
