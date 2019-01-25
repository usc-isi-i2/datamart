from bs4 import BeautifulSoup
from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor


class HTMLProcesser(object):

    TITLE_BLACK_LIST = {
        'excel',
        'csv',
        'file',
        'download',
        'table',
        'data',
        'save'
    }

    def __init__(self, html):
        self.html_text = self.load_html_soup(html)

    def extract_description_from_meta(self):
        descriptions = []
        extractor = HTMLMetadataExtractor()
        extractions = extractor.extract(self.html_text, extract_title=True, extract_meta=True)
        for extraction in extractions:
            if extraction.tag == 'title':
                descriptions.append(extraction.value.strip())
            elif extraction.tag == 'meta':
                meta_description = extraction.value.get('description')
                descriptions.append(meta_description)
        return '\n'.join(descriptions)

    def generate_a_tags_from_html(self):
        soup = BeautifulSoup(self.html_text, features="lxml")
        if soup.find('body'):
            for a in soup.find('body').find_all('a', href=True):
                yield a.text, a['href']

    @staticmethod
    def load_html_soup(html):
        if html.startswith('http'):
            import urllib.request
            f = urllib.request.urlopen(html)
            html_text = f.read().decode('utf-8')
        elif html.endswith('.html'):
            with open(html) as f:
                html_text = f.read()
        else:
            html_text = html
        return html_text
