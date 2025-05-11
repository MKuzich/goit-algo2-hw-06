from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import requests
import matplotlib.pyplot as plt
import string
import re

def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching text: {e}")
        return None

def preprocess(text):
    text = text.lower()
    text = re.sub(rf"[{re.escape(string.punctuation)}]", "", text)
    words = text.split()
    return words

def map_function(word):
    return word, 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

def map_reduce(text):
    words = preprocess(text)

    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    shuffled_values = shuffle_function(mapped_values)

    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

def visualize_top_words(word_freq, top_n=10):
    top_items = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:top_n]
    words, counts = zip(*top_items)

    plt.figure(figsize=(10, 6))
    plt.bar(words, counts, color='skyblue')
    plt.title(f"Top {top_n} Most Frequent Words")
    plt.xlabel("Words")
    plt.ylabel("Frequency")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    url = "https://www.gutenberg.org/files/1250/1250-0.txt"
    text = get_text(url)
    if text:
        result = map_reduce(text)
        visualize_top_words(result)
    else:
        print("Помилка: Не вдалося отримати вхідний текст.")