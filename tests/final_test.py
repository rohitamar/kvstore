from random import choices, randint
import requests 
import string

testcases = 5000
real_map = {}

def get_random_string():
    n = randint(100, 150)
    return "".join(
        choices(string.ascii_uppercase, k=n)
    )

session = requests.Session()

for _ in range(testcases):
    key = get_random_string()
    value = get_random_string()

    res = requests.put(
        url='http://127.0.0.1:8080/insert',
        json={
            "key": key,
            "value": value
        }
    )
    real_map[key] = value 

for key in real_map.keys():
    res = requests.get(
        url=f'http://127.0.0.1:8080/get/{key}'
    )

    actual_value = real_map[key]
    data = res.json()
    db_value = data.get('payload')

    if db_value != actual_value:
        print(f'For key {key}:')
        print(f'db_value: {db_value}')
        print(f'actual_value: {actual_value}')
