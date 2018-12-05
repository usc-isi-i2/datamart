from datamart.materializers.wikitables_downloader.utils import *
from datamart.materializers.wikitables_downloader.article_downloader import *
from datamart.materializers.wikitables_downloader.table_processing import *

def articles_with_tables(lang='en', download='recent'):
	''' Download the Wikipedia corpus, parses the articles looking for tables,
	and generates a CSV with article names and number of tables. This may take
	up to 20 hours, and Wikipedia generates a new dump every 2 weeks.
	Arguments:
	lang -- the language code of the Wikipedia site.
	download -- the download strategy: 'recent' generate a CSV if a new dump is
	available, 'skip' use the previous CSV if any, 'force' generates a CSV even
	if the most recent is already available. '''
	dumps = listdir(PATH_RESOURCES)
	dumps = [f for f in dumps if search(PATTERN_FIND_ARTICLES % lang, f)]
	if download == 'skip' and len(dumps):
		path_csv = join(PATH_RESOURCES, list(sorted(dumps))[-1])
	else:
		dump_url = locate_dump(lang, URL_WIKIPEDIA_MIRROR)
		input_fname = join(PATH_RESOURCES, dump_url.rsplit('/')[-1])
		date = input_fname.split('-')[1]
		path_csv = PATH_ARTICLES % (lang, date)
		if download == 'force' or not exists(path_csv):
			log('info', f'Downloading latest {lang} Wikipedia dump.')
			download_file(dump_url, input_fname, chunk_size=2000000)
			log('info', f'Generating CSV from the {lang} dump.')
			generate_csv(input_fname, path_csv, lang)
			log('info', f'Done. Deleting Wikipedia dump')
			remove(input_fname)
	with open(path_csv, 'r', encoding='utf-8') as fp:
		articles = fp.read().strip().split("\n")[2:]
		articles = [a.rsplit(',', 1) for a in articles]
		articles = {a[0][1:-1].replace('\\"', '"'): int(a[1]) for a in articles}
	return articles

def tables(article, lang='en', store_result=False, xpath=None, cache_time=24 * 3600):
	''' Given a Wikipedia article name or URL, returns a list of Tables. '''
	if article.startswith('http'):
		url = article
	else:
		url = f'https://{lang}.wikipedia.org/wiki/{article.replace(" ", "_")}'
	url = url.split('#', 1)[0]
	document = cache(get_with_render, (url, SELECTOR_ROOT), identifier=url)
	document = soup(document, 'html.parser')
	article = document.title.text.rsplit('-', 1)[0].strip()
	res = []
	for table in locate(url, document):
		if xpath == None or table.xpath == xpath:
			try:
				segmentate(table)
				if len(table.features) < 2 or len(table.features[0]) < 2: continue
				functional_analysis(table)
				structural_analysis(table)
				interpret(table)
				compute_score(table)
			except:
				log_error()
				table.error = format_exc()
			res.append(table)
	if len(res) and store_result:
		html_tables = ''
		for table in res:
			with open(PATH_ALL_TABLES, 'a', encoding='utf-8') as fp:
				fp.write(f'{table.json()}\n')
			html_tables += table.html()
		with open(f"{PATH_HTML_ARTICLES}/{fname_escape(url.split('/wiki/', 1)[1].replace('_', ' '))[:100]}.html", 'w', encoding='utf-8') as fp:
			fp.write(HTML_ARTICLE_TEMPLATE % (article, url, article, date_stamp(), html_tables))
	if xpath != None:
		if len(res):
			res = res[0]
		else:
			res = None
	return res