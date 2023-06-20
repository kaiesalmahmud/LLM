import os
import openai

def read_api_key():
    with open('key.txt', 'r') as file:
        api_key = file.read().strip()
    return api_key

api_key = read_api_key()
openai.api_key = api_key

def query_generator(context, prompt):

	response = openai.Completion.create(
		model="text-davinci-003",
		prompt=context+prompt,
		temperature=0,
		max_tokens=150,
		top_p=1.0,
		frequency_penalty=0.0,
		presence_penalty=0.0,
		stop=["#", ";"]
		)

	query = response.choices[0].text

	return query
context = "### Postgres SQL tables, with their properties:\n#\n# Employee(id, name, department_id)\n# Department(id, name, address)\n# Salary_Payments(id, employee_id, amount, date)\n#\n"
prompt = "### A query to list the names of the departments which employed more than 10 employees in the last 3 months\n"
query = query_generator(context, prompt)

print(context)
print(prompt)
print(query)