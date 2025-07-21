import requests

def generate_doc_from_local_model(prompt, model="llama3"):
    url = "http://localhost:11434/api/generate"
    response = requests.post(url, json={
        "model": model,
        "prompt": prompt,
        "stream": False
    })
    return response.json()["response"]
