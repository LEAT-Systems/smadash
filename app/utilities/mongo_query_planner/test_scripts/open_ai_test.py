from openai import OpenAI
import os
from dotenv import load_dotenv

# Load variables from .env file into environment
load_dotenv()

# Access them like normal environment variables
API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=API_KEY)

response = client.responses.create(
    model="gpt-5",
    input="Write a one-sentence bedtime story about a unicorn."
)

print(response.output_text)