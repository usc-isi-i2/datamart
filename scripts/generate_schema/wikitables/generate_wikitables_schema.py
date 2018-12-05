from argparse import ArgumentParser
from datamart.materializers.wikitables_downloader.wikitables import tables, articles_with_tables
from json import dump
from os import makedirs
from os.path import join
from regex import sub
from requests import head
from pandas import DataFrame

RANDOM_URL = 'https://%s.wikipedia.org/wiki/Special:Random'
SCORE_THRESHOLD = 0.8

def random_article(lang='en'):
    return head(RANDOM_URL % lang, allow_redirects=True).url

def generate_datasets(path, language, url, xpath=None):
    makedirs(path, exist_ok=True)
    tabs = tables(url, language, False, xpath)
    if xpath == None:
        tabs = [t for t in tabs if t.score > SCORE_THRESHOLD]
    name = sub(r'[/\\\*;\[\]\':=,<>]', '_', url)
    for t, tab in enumerate(tabs):
        with open(join(path, f'{name}_{t}.json'), 'w', encoding='utf-8') as fp:
            dump(tab.metadata(), fp, ensure_ascii=False, indent='\t')
    return len(tabs)

if __name__ == '__main__':
    ap = ArgumentParser(description='Generate one metadata json for each table in a Wikipedia article.\n\tUSAGE: python3 generate_wikitables_schema.py -lang ie -a')
    ap.add_argument('-p', '--path', help='Directory where metadata files should be stored. The script dirname by default.', default='datasets', type=str)
    ap.add_argument('-l', '--lang', help='Wikipedia 2-letter language code. English by default.', default='en', type=str)
    ap.add_argument('-u', '--url', help='URL or article name. A random article by default.', type=str)
    ap.add_argument('-a', '--all', help='If specified, download the whole corpus for the language. This process can take +20 hours depending on the language, speed connection and processor.', default=False, action='store_true')
    ap.add_argument('-x', '--xpath', help='XPath of the table in the Wikipedia article. If not specified, every table is processed.', type=str)
    args = ap.parse_args()
    if args.all:
        articles = articles_with_tables(args.lang)
        for a, art in enumerate(articles):
            print(f'({a + 1}/{len(articles)}) {art}')
            generate_datasets(args.path, args.lang, art)
    else:
        if args.url == None:
            while True:
                url = random_article(args.lang)
                print(url)
                if generate_datasets(args.path, args.lang, url):
                    break
        else:
            tables = generate_datasets(args.path, args.lang, args.url, args.xpath)