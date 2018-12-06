from datamart.materializers.wikitables_downloader.utils import *

PATTERN_FIND_ARTICLES = r'%s_(\d+)_articles.csv'
PATTERN_FIND_TABLES = compile(r'^([ :]*+){\|(?:(?!^\ *+\{\|).)*?\n\s*+(?> \|} | \Z )', DOTALL | MULTILINE | VERBOSE).findall  # From wikitextparser source
PATH_ARTICLES = join(PATH_RESOURCES, '%s_%s_articles.csv')
URL_WIKIPEDIA_MIRROR = 'https://dumps.wikimedia.org/backup-index.html'
URL_WIKIPEDIA_PAGES_COUNT = 'https://%s.wikipedia.org/w/api.php?action=parse&format=json&contentmodel=wikitext&text={{NUMBEROFPAGES}}'

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
	count = soup(count['parse']['text']['*'], "html.parser").text
	return int(sub(r"\D", "", count))

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