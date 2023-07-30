import openai

def read_api_key():
    with open('key.txt', 'r') as file:
        api_key = file.read().strip()
    return api_key

api_key = read_api_key()
openai.api_key = api_key

def is_api_key_valid():
    try:
        response = openai.Completion.create(
            engine="davinci",
            prompt="This is a test.",
            max_tokens=5
        )
    except:
        return False
    else:
        return True

# Check the validity of the API key
api_key_valid = is_api_key_valid()
print("API key is valid:", api_key_valid)