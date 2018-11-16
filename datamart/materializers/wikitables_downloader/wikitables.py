from datamart.materializers.wikitables_materializer.utils import *
from datamart.materializers.wikitables_materializer.table_processing import *


def tables(article, lang='en', store_result=True, xpath=None):
    ''' Given a Wikipedia article name or URL, returns a list of Tables. '''
    if article.startswith('http'):
        url = article
        article = url.rsplit('/', 1)[-1].replace('_', ' ')
    else:
        url = f'https://{lang}.wikipedia.org/wiki/{article.replace(" ", "_")}'
    document = cache(get_with_render, (url, SELECTOR_ROOT), identifier=url)
    res = []
    for table in locate(url, soup(document, 'html.parser')):
        if xpath != None and table.xpath != xpath: continue
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
        with open(f'{PATH_HTML_ARTICLES}/{article}.html', 'w', encoding='utf-8') as fp:
            fp.write(HTML_ARTICLE_TEMPLATE % (article, url, article, date_stamp(), html_tables))
    if xpath != None:
        res = res[0] if len(res) else None
    return res
