from os.path import join
from argparse import ArgumentParser
from os import makedirs
from json import dump
from datamart.materializers.wikitables_downloader.wikitables import tables

RANDOM_WIKIPEDIA_ARTICLE = 'https://en.wikipedia.org/wiki/Special:Random'
SCORE_THRESHOLD = 0.9

def generate_json_schema(path, article):
    if path == None:
        print('Output path not specified. Using this folder.')
        path = '.'
    makedirs(path, exist_ok=True)
    if article == None:
        print('No article was specified. Looking for a random article that contain tables.')
        article = RANDOM_WIKIPEDIA_ARTICLE
    tabs = []
    while not len(tabs):
        tabs = [t.dataframe() for t in tables(article) if t.quality_score > SCORE_THRESHOLD]
    for t, table in enumerate(tabs):
        with open(join(path, f'{t}.json'), 'w', encoding='utf-8') as fp:
            dump(table, fp, ensure_ascii=False, indent='\t')

if __name__ == '__main__':
    parser = ArgumentParser(description='Generate as one metadata json for each database in a Wikipedia article.')
    parser.add_argument("-p", "--path", help='blabla', type=str)
    parser.add_argument("-a", "--article", help='Article name or URL.', type=str)
    parser_args = parser.parse_args()
    generate_json_schema(parser_args.dst, parser_args.article)