from traceback import print_exc
from argparse import ArgumentParser
from bs4 import BeautifulSoup as soup
from bz2 import BZ2File
from datetime import datetime as dt
from json import dump
from os import makedirs, listdir, remove
from os.path import join, exists
from pandas import DataFrame
try:
    from etk.extractors.spacy_ner_extractor import SpacyNerExtractor
except OSError:
    from spacy.cli import download
    download('en_core_web_sm')
    from etk.extractors.spacy_ner_extractor import SpacyNerExtractor
from regex import compile as rx_compile, findall, search, sub, DOTALL, MULTILINE, VERBOSE
from requests import head, get
from tablextract import tables, BOOLEAN_SYNTAX_PROPERTIES, STOPWORDS
from tablextract.utils import find_dates, download_file
from time import time
from urllib.parse import urljoin
from wikipediaapi import Wikipedia
from xml.etree.ElementTree import iterparse

# --- entrypoint --------------------------------------------------------------

def main():
    ap = ArgumentParser(description='Generate one metadata json for each table in a Wikipedia article.\n\tUSAGE: python3 generate_wikitables_schema.py -lang ie -a')
    ap.add_argument('-u', '--url', help='URL of a Wikipedia article. A random article by default.', type=str)
    ap.add_argument('-x', '--xpath', help='Extract only tables that matches a specfic XPath.', type=str)
    ap.add_argument('-l', '--lang', help='Wikipedia 2-letter language code. English (en) by default.', default='en', type=str)
    ap.add_argument('-s', '--score', help='Score threshold in [0, 1] to discard datasets with an expected quality lower than the score. Set to 0.8 by default.', default=.8, type=float)
    ap.add_argument('-p', '--path', help='Directory where metadata files should be stored. The script dirname by default.', default='datasets', type=str)
    ap.add_argument('-a', '--all', help='If specified, download the whole corpus for the language. This process can take +20 hours depending on the language, speed connection and CPU.', default=False, action='store_true')

    args = ap.parse_args()

    makedirs(args.path, exist_ok=True)

    if args.all:
        articles = articles_with_tables(args.path, args.lang)
        for a, art in enumerate(articles):
            print('(%d/%d) %s' % (a + 1, len(articles), art))
            url = 'https://%s.wikipedia.org/wiki/%s' % (args.lang, art.replace(' ', '_'))
            generate_datasets(url, args.path, args.score, args.lang)
    else:
        if args.url == None:
            while True:
                url = random_article(args.lang)
                if generate_datasets(url, args.path, args.score):
                    break
        else:
            generate_datasets(args.url, args.path, args.score, args.xpath)

# --- table downloading -------------------------------------------------------

WIKIPEDIA_IGNORE_CATEGORIES = ['articles', 'cs1', 'page', 'dates', ' use', 'wikipedia', 'links']
WIKIPEDIA_CSS_FILTER = '#content table:not(.infobox):not(.navbox):not(.navbox-inner):not(.navbox-subgroup):not(.sistersitebox)'
RANDOM_URL = 'https://%s.wikipedia.org/wiki/Special:Random'


def generate_datasets_metadata(url, score_threshold=0.8, xpath=None):
    print('Downloading %s' % url)
    result = []
    tabs = tables(url, xpath_filter=xpath, css_filter=WIKIPEDIA_CSS_FILTER)
    if xpath is None:
        tabs = [t for t in tabs if t.score > score_threshold]
    for table in tabs:
        result.append(metadata(table))
    return result


def generate_datasets(url, path, score_threshold, xpath=None):
    print('Downloading %s' % url)
    tabs = tables(url, xpath_filter=xpath, css_filter=WIKIPEDIA_CSS_FILTER)
    if xpath == None:
        tabs = [t for t in tabs if t.score > score_threshold]
    name = sub(r'[/\\\*;\[\]\':=,<>]', '_', url)
    for t, table in enumerate(tabs):
        print(metadata(table))
        with open(join(path, '%s_%d.json' % (name, t)), 'w', encoding='utf-8') as fp:
            dump(metadata(table), fp, ensure_ascii=False, indent='\t')
    print('\t%d datasets found.' % len(tabs))
    return len(tabs)

def metadata(table, min_majority=.8):
    ''' Returns a datamart schema, assigning types to each variable, if at
    least min_majority of the values are of that type. '''
    lang = table.url.split('.', 1)[0].split('/')[-1]
    pg = Wikipedia(lang).page(table.url.rsplit('/', 1)[-1])
    try:
        date_updated = pg.touched
    except:
        date_updated = dt.now().strftime('%Y-%m-%mT%H:%M:%SZ')
    try:
        categories = {kw.lower().split(':', 1)[-1].strip() for kw in pg.categories}
        categories = {c for c in categories if not any(i in c for i in WIKIPEDIA_IGNORE_CATEGORIES)}
        keywords = {kw for cat in categories for kw in findall(r'\w+', cat)}
        keywords = {kw for kw in keywords if kw not in STOPWORDS}
    except:
        categories = []
        keywords = []
    try:
        description = pg.summary.split('\n', 1)[0]
    except:
        description = ''
    try:
        langlinks = list({v.title for v in pg.langlinks.values()})
    except:
        langlinks = []
    headings = [(k[2:], v.replace('[edit]', '')) for k, v in table.context.items() if k.startswith('h')]
    res = {
        'title': table.context['r0'] if 'r0' in table.context else 'Table in %s' % pg.title,
        'description': description,
        'url': table.url,
        'keywords': list(keywords),
        'date_updated': date_updated,
        'provenance': {
            'source': 'wikipedia.org'
        },
        'materialization': {
            'python_path': 'wikitables_materializer',
            'arguments': {
                'url': table.url,
                'xpath': table.xpath
            }
        },
        'additional_info': {
            'categories': list(categories),
            'sections': [s.title for s in pg.sections],
            'translations': langlinks,
            'context_data': {k: v for k, v in table.context.items() if not k.startswith('h')},
            'headings': [h[1] for h in sorted(headings)]
        }
    }
    res['variables'] = []
    for name in table.record[0].keys():
        var = {'name': name, 'semantic_type': []}
        values = [r[name] for r in table.record]
        min_sample = min_majority * len(values)
        dates = [d for d in map(find_dates, values) if d != None]
        if len(dates) >= min_sample:
            var['semantic_type'].append('https://metadata.datadrivendiscovery.org/types/Time')
            var['temporal_coverage'] = {
                'start': min(dates).isoformat(),
                'end': max(dates).isoformat()
            }
        entities = {v: t for v in values for v, t in find_entities(v).items()}
        locations = [v for v, t in entities.items() if t == 'GPE']
        if len(locations) >= min_sample:
            var['semantic_type'].append('https://metadata.datadrivendiscovery.org/types/Location')
        people = [v for v, t in entities.items() if t == 'PERSON']
        if len(people) >= min_sample:
            var['semantic_type'].append('https://schema.org/Person')
        if len(entities) >= min_sample:
            var['named_entity'] = list(entities.keys())
        numbers = [float(n) for n in values if n.strip().replace('.', '', 1).isdigit()]
        ranges = [n for n in values if BOOLEAN_SYNTAX_PROPERTIES['match-range'](n) is not None]
        if len(numbers) >= min_sample:
            var['semantic_type'].append('http://schema.org/Float')
        elif len(ranges) >= min_sample:
            var['semantic_type'].append('https://metadata.datadrivendiscovery.org/types/Interval')
        if not len(var['semantic_type']):
            if any(len(c) for c in values):
                var['semantic_type'].append('http://schema.org/Text')
            else:
                var['semantic_type'].append('https://metadata.datadrivendiscovery.org/types/MissingData')
        res['variables'].append(var)
    return res

def random_article(lang='en'):
    return head(RANDOM_URL % lang, allow_redirects=True).url

# --- Wikipedia exploration ---------------------------------------------------

PATTERN_FIND_ARTICLES = r'%s_(\d+)_articles.csv'
PATTERN_FIND_TABLES = rx_compile(r'^([ :]*+){\|(?:(?!^\ *+\{\|).)*?\n\s*+(?> \|} | \Z )', DOTALL | MULTILINE | VERBOSE).findall  # From wikitextparser source
PATH_ARTICLES = '%s_%s_articles.csv'
URL_WIKIPEDIA_MIRROR = 'https://dumps.wikimedia.org/backup-index.html'
URL_WIKIPEDIA_PAGES_COUNT = 'https://%s.wikipedia.org/w/api.php?action=parse&format=json&contentmodel=wikitext&text={{NUMBEROFPAGES}}'

def articles_with_tables(resources_path, lang='en', download='recent'):
    ''' Download the Wikipedia corpus, parses the articles looking for tables,
    and generates a CSV with article names and number of tables. This may take
    up to 20 hours, and Wikipedia generates a new dump every 2 weeks.
    Arguments:
    lang -- the language code of the Wikipedia site.
    download -- the download strategy: 'recent' generate a CSV if a new dump is
    available, 'skip' use the previous CSV if any, 'force' generates a CSV even
    if the most recent is already available. '''
    dumps = listdir(resources_path)
    dumps = [f for f in dumps if search(PATTERN_FIND_ARTICLES % lang, f)]
    if download == 'skip' and len(dumps):
        path_csv = join(resources_path, list(sorted(dumps))[-1])
    else:
        dump_url = locate_dump(lang, URL_WIKIPEDIA_MIRROR)
        input_fname = join(resources_path, dump_url.rsplit('/')[-1])
        date = input_fname.split('-')[1]
        path_csv = join(resources_path, PATH_ARTICLES) % (lang, date)
        if download == 'force' or not exists(path_csv):
            print('info', f'Downloading latest {lang} Wikipedia dump.')
            download_file(dump_url, input_fname, chunk_size=2000000)
            print('info', f'Generating CSV from the {lang} dump.')
            generate_csv(input_fname, path_csv, lang)
            print('info', f'Done. Deleting Wikipedia dump')
            remove(input_fname)
    with open(path_csv, 'r', encoding='utf-8') as fp:
        articles = fp.read().strip().split('\n')[2:]
        articles = [a.rsplit(',', 1) for a in articles]
        articles = {a[0][1:-1].replace('\\"', '"'): int(a[1]) for a in articles}
    return articles

def locate_dump(lang, mirror):
    ''' Locate the URL of the last Wikipedia dump or the previous one when the
    current is being downloaded. '''
    all_wikis = soup(get(mirror).text, 'html.parser')
    lang_wiki_link = urljoin(mirror, all_wikis.find('a', text=f'{lang}wiki')['href'])
    lang_wiki = soup(get(lang_wiki_link).text, 'html.parser')
    if 'complete' not in lang_wiki.find('p', {'class': 'status'}).text:
        previous_link = next(a for a in lang_wiki.find_all('a') if 'Last dumped' in a.text or 'previous dump' in a.text)['href']
        lang_wiki = soup(get(urljoin(lang_wiki_link + '/', previous_link)).text, 'html.parser')
    dump_url = next(a for a in lang_wiki.find_all('a') if 'pages-articles.xml.bz2' in a.text)['href']
    return urljoin(mirror, dump_url)

def number_of_pages(lang):
    count = get(URL_WIKIPEDIA_PAGES_COUNT % lang).json()
    count = soup(count['parse']['text']['*'], 'html.parser').text
    return int(sub(r'\D', '', count))

def generate_csv(input_fname, output_fname, lang):
    ''' Iteratively parse Wikipedia xml.bz2 file and generate a CSV with the
    articles that contain tables. '''
    to_write = []
    pages_visited = 0
    start = time()
    ns = None
    total_pages = number_of_pages(lang)
    with BZ2File(input_fname) as bzf:
        with open(output_fname, 'w', encoding='utf-8') as out:
            out.write('sep=,\narticle_title,table_count\n')
        for ev, elem in iterparse(bzf, events=('start', 'end')):
            if ev == 'start':
                if ns == None:
                    root = elem
                    ns = elem.tag[:-len('mediawiki')]
            else:
                if elem.tag == f'{ns}page':
                    pages_visited += 1
                    if elem.find(f'{ns}ns').text != '0' or elem.find(f'{ns}redirect'): continue
                    try:
                        tables = len(PATTERN_FIND_TABLES(elem.find(f'*/{ns}text').text))
                    except:
                        tables = 0
                    if tables:
                        title = elem.find(f'{ns}title').text.replace('"', '\\"')
                        to_write.append('"%s",%s' % (title, tables))
                        if len(to_write) > 1000:
                            with open(output_fname, 'a', encoding='utf-8') as out:
                                out.write('\n'.join(to_write + ['']))
                            to_write = []
                            eta = (total_pages - pages_visited) * (time() - start) / pages_visited
                            eta = str(timedelta(seconds=int(eta))).zfill(8)
                            print('Completion: %04.2f%%, eta: %s' % (100 * pages_visited / total_pages, eta))
            root.clear()
        if len(to_write):
            with open(output_fname, 'a', encoding='utf-8') as out:
                out.write('\n'.join(to_write + ['']))

_find_entities_extractor = SpacyNerExtractor('dummy_parameter')
def find_entities(text):
    try:
        return {ext.value: ext.tag for ext in _find_entities_extractor.extract(text)}
    except:
        log('info', f'ETK SpacyNerExtractor raised an error on value {text}.')
        return {}

# --- initial call ------------------------------------------------------------

if __name__ == '__main__':
    main()
