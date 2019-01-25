import typing
from datamart.index_builder import IndexBuilder
from datamart.utilities.html_processer import HTMLProcesser
from datamart.utilities.utils import Utils, DEFAULT_ES
from datamart.materializers.general_materializer import GeneralMaterializer


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
    'mp3',
    'mp4',
    'avi',
    'wmv'
}


def generate_metadata(description: dict, es_index: str=None) -> typing.List[dict]:
    """
    Step 1 for indexing, user provide a description with url for materializing,
    datamart will try to generate metadata, by materializing, profiling the data,
    and will return to the users to take a look or edit for final indexing.

    :param description: a dict, must have the key "materializer_arguments",
           description["materializer_arguments"] must have the key "url" which is a valid url pointing to the real data
    :return: List of dict(mostly only one dict in the list, unless cases like excel file with multiple sheets)
             Each dict is a metadata that can be indexed to elasticsearch
    """

    url = description['materialization_arguments']['url'].rstrip('/')
    if not (url and isinstance(url, str) and Utils.validate_url(url)):
        return []

    file_name = url.rsplit('/', 1)[-1].rsplit('.', 1)[0]
    if not file_name:
        return []
    if len(file_name) == 2:
        file_suffix = file_name[1]
        if file_suffix.lower() in FILE_BLACK_LIST:
            return []

    file_name = file_name[0].replace('-', ' ').replace('_', ' ')
    if not description.get('title'):
        description['title'] = file_name

    if not description.get('url'):
        description['url'] = url

    description['materialization'] = {
        'python_path': 'general_materializer',
        'arguments': description['materialization_arguments']
    }
    del description['materialization_arguments']

    ib = IndexBuilder()

    meta_list = []
    parse_results = GeneralMaterializer().parse(description)
    for res in parse_results:
        try:
            df = res.dataframe
            idx = res.index
            sub_name = '(%s)' % res.name if res.name else ''
            if sub_name or description.get('title'):
                description['title'] = description.get('title', '') + sub_name
            description['materialization']['arguments']['index'] = idx or 0
            # TODO: make use of res.metadata?
            indexed = ib.indexing_generate_metadata(
                description_path=description,
                es_index=es_index or DEFAULT_ES,
                data_path=df
            )
            meta_list.append(indexed)
        except Exception as e:
            print('user_upload_indexing, FAIL ON %d' % res.index, e)
            continue
    return meta_list


def upload(meta_list: typing.List[dict], es_index: str=None) -> typing.List[dict]:
    ib = IndexBuilder()
    successed = []
    for meta in meta_list:
        try:
            success = ib.indexing_send_to_es(metadata=meta, es_index=es_index)
            if success:
                successed.append(meta)
        except:
            pass
    return successed


def bulk_generate_metadata(html_page: str,
                           description: dict=None,
                           es_index: str=None) -> typing.List[typing.List[dict]]:
    """

    :param html_page:
    :param description:
    :param es_index:
    :return:
    """
    successed = []
    description = description or {}
    hp = HTMLProcesser(html_page)
    meta = hp.extract_description_from_meta()
    for text, href in hp.generate_a_tags_from_html():
        try:
            if not Utils.validate_url(href):
                continue
            if not description.get('title'):
                black_list = set(text.split()).intersection(hp.TITLE_BLACK_LIST)
                if not black_list:
                    description['title'] = text.strip()
            if not description.get('description'):
                description['description'] = meta
            if not description.get('url'):
                description['url'] = href
            description['materialization_arguments'] = {'url': href}
            meta = generate_metadata(description, es_index)
            successed.append(meta)
        except Exception as e:
            print(' - FAILED GENERATE METADATA ON \n\ttext = %s, \n\thref = %s \n%s' % (text, href, str(e)))
    return successed


def bulk_upload(html_page: str, description: dict=None, es_index: str=None) -> typing.List[typing.List[dict]]:
    """
    extract links from html page and index each of the data

    Args:
        html_page
        description:

    Returns:

    """
    list_of_meta_list = bulk_generate_metadata(html_page, description, es_index)

    successed = []
    for meta_list in list_of_meta_list:
        success_list = upload(meta_list, es_index)
        if success_list:
            successed.append(success_list)
    return successed
