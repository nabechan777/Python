import re
import os
import time

import requests
import lxml.html
from pymongo import MongoClient

MONGODB_USER = os.environ['MONGODB_USER']
MONGODB_PASSWORD = os.environ['MONGODB_PASSWORD']
MONGODB_HOST = os.environ['MONGODB_HOST']

def get_mongodb_collection():
    """ MongoDBに接続し、ebooksコレクションを返す。ユーザIDとパスワードは.envに記載
    """
    # uri = 'mongodb://python-designated:python-pwd@localhost:27017/gihyo'
    uri = 'mongodb://%s:%s@%s/gihyo' % ( MONGODB_USER, MONGODB_PASSWORD, MONGODB_HOST )
    client = MongoClient(uri)
    collection = client.gihyo.ebooks
    collection.create_index('key', unique=True)
    return collection

def scrape_list_page(rep):
    """一覧ページをスクレイピング

    Parameters
    ----------
    rep : Response
        一覧ページのResponse

    Returns
    -------
    generator
        詳細ページのURL群

    """

    # 一覧ページを取得し、リンクを相対URLから絶対URLに変換する。
    root = lxml.html.fromstring(rep.content)
    root.make_links_absolute(rep.url)

    # 詳細ページのURLを抽出し、それらURLのジェネレータを返す。
    for a in root.cssselect('#listBook a[itemprop="url"]'):
        url = a.get('href')
        yield url

def scrape_detail_page(rep):
    """詳細ページをスクレイピング

    Parameters
    ----------
    rep : Response
        詳細ページのレスポンス

    Returns
    -------
    dict
        電子書籍情報（URL・タイトル・価格・内容＜目次＞）

    """
    # 詳細ページを取得し、任意のデータを抽出
    root = lxml.html.fromstring(rep.content)
    ebook = {
        'url'    : rep.url,
        'key'    : extract_key(rep.url),
        'title'  : root.cssselect('#bookTitle')[0].text_content(),
        'price'  : root.cssselect('.buy')[0].text.strip(),
        'content': [nomalize_spaces(h3.text_content()) for h3 in root.cssselect('#content > h3')]
    }
    return ebook

def nomalize_spaces(s):
    """対象の文字列から連続する空白と前後の空白を取り除く

    Parameters
    ----------
    s : str
        対象の文字列

    Returns
    -------
    str
        連続する空白と前後の空白を取り除いた文字列

    """
    return re.sub(r'\s+', ' ', s).strip()

def extract_key(url):
    return re.search(r'/([^/]+)$', url).group(1)

def main():
    """クローラーのメイン処理（クロール＞スクレイピング＞MongoDB）
    """

    # ebooksコレクションを取得
    collection = get_mongodb_collection()

    # 対象のWebサイトに接続
    session = requests.Session()
    response = session.get('https://gihyo.jp/dp')

    # 一覧ページをスクレイピング
    urls = scrape_list_page(response)

    # 詳細ページをスクレイピングした後、DBに登録
    for url in urls:
        key = extract_key(url)
        ebook = collection.find_one({'key': key})

        if not ebook:
            time.sleep(1)
            response = session.get(url)
            ebook = scrape_detail_page(response)
            collection.insert_one(ebook)

        print(ebook)

if __name__ == '__main__':
    main()
