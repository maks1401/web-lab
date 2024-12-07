import requests
import json
from urllib.parse import quote_plus, unquote_plus, unquote
from warcio import ArchiveIterator
from bs4 import BeautifulSoup

def search_ru_wiki(index_name):
    #Ищет записи в архиве Common Crawl по указанному индексу
    url = 'ru.wikipedia.org/*'
    encoded_url = quote_plus(url)
    index_url = f'http://index.commoncrawl.org/{index_name}-index?url={encoded_url}&output=json'
    response = requests.get(index_url)

    if response.status_code == 200:
        records = response.text.strip().split('\n')
        return [json.loads(record) for record in records]
    else:
        print(f"Ошибка при поиске: {response.status_code}")
        return []

def fetch_single_record(warc_record_filename, offset, length):
    #Извлекает отдельную запись из WARC файла
    s3_url = f'https://data.commoncrawl.org/{warc_record_filename}'
    byte_range = f'bytes={offset}-{offset + length - 1}'
    response = requests.get(s3_url, headers={'Range': byte_range}, stream=True)

    if response.status_code == 206:
        stream = ArchiveIterator(response.raw)
        for warc_record in stream:
            if warc_record.rec_type == 'response':
                return warc_record.content_stream().read()
    else:
        print(f"Не удалось получить данные: {response.status_code}")
    
    return None

def main():
    # Поиск записей по индексам
    indices = ['CC-MAIN-2024-38', 'CC-MAIN-2024-33', 
               'CC-MAIN-2024-30', 'CC-MAIN-2024-26', 
               'CC-MAIN-2024-22']
    
    results = []
    for index in indices:
        results += search_ru_wiki(index)

    # Удаление дубликатов
    unique_results = {result['url']: result for result in results}

    #print (len (results))
    #print (len (unique_results))

    # Ключевые слова для фильтрации
    keywords = [
        'Пермский'
    ]

    # Фильтрация результатов по ключевым словам
    filtered_results = [
        result for result in unique_results.values()
        if any(keyword.casefold() in unquote(result['url']).casefold() for keyword in keywords)
    ]

    print ("Кол-во результатов:", len(filtered_results))

    # Извлечение HTML-контента
    html_results = {}
    for result in filtered_results:
        record = fetch_single_record(result['filename'], int(result['offset']), int(result['length']))
        if record:
            html_results[result['url']] = record

    # Обработка и вывод результатов
    for url, html in html_results.items():
        beautiful_soup = BeautifulSoup(html, 'html.parser')
        print(f"Статья - {beautiful_soup.title.string}; URL - {unquote_plus(url)}")

        for keyword in keywords:
            if keyword.casefold() in beautiful_soup.get_text().casefold():
                print(f"Ключевое слово: {keyword} в статье {beautiful_soup.title.string}")

        print("---------------------------------------")

if __name__ == "__main__":
    main()
