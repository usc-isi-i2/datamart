from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datamart.utilities.utils import Utils
try:
    from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor
except OSError:
    from spacy.cli import download
    download('en_core_web_sm')
    from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor


FILE_BLACK_LIST = {
    'pdf',
    'zip',
    'tar',
    'gz',
    'jpg',
    'jpeg',
    'png',
    'img',
    'gif',
    'doc',
    'docx',
    'ppt',
    'pptx',
    'mp3',
    'mp4',
    'avi',
    'wmv',
    'flv',
    '3gp',
    'svg',
    'dxf',
    'psd',
    'php',  # NOT EXTRACT HTML TABLE WHEN BULK UPLOAD
    'html'  # OTHERWISE TOO MANY FP WILL BE INCLUDED
}

TITLE_BLACK_LIST = {
    'excel',
    'csv',
    'file',
    'download',
    'table',
    'data',
    'save',
    'click',
    'index'
}.union(FILE_BLACK_LIST)


class HTMLProcesser(object):

    def __init__(self, html):
        self.url = None
        if html.startswith('http') or html.startswith('ftp'):
            if Utils.validate_url(html):
                self.url = html
        self.html_text = self.load_html_text(html)

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
                href = a['href']
                if not (href.startswith('http') or href.startswith('ftp')):
                    # relative url
                    if self.url:
                        href = urljoin(self.url, href)
                    else:
                        continue
                yield a.text, href

    def load_html_text(self, html):
        html_text = html
        if self.url:
            import urllib.request
            f = urllib.request.urlopen(html)
            html_text = f.read().decode('utf-8', errors='ignore')
        elif html.endswith('.html'):
            try:
                with open(html) as f:
                    html_text = f.read()
            except:
                pass
        return html_text
