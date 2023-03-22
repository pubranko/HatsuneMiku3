from bs4 import BeautifulSoup as bs4
from bs4.element import Tag
from dateutil.parser import parse
from shared.settings import TIMEZONE
import requests
from BrownieAtelierMongo.data_models.scraper_info_by_domain_data import ScraperInfoByDomainConst


def scraper(soup: bs4, scraper: str, scrape_parm: list[dict[str, str]]) -> tuple[dict, dict]:
    ''' '''
    scraped_result: dict = {}
    scraped_pattern: dict = {}
    scraped_item = None
    scrape_info: dict = {}
    ### cssセレクターでスクレイプ対象を取得できるまで繰り返し ###
    for scrape_info in scrape_parm:
        scraped_item = soup.select_one(scrape_info[ScraperInfoByDomainConst.ITEM__CSS_SELECTER])
        if type(scraped_item) is Tag:
            scraped_pattern = {scraper: scrape_info[ScraperInfoByDomainConst.ITEM__PATTERN]}
            scraped_result['publish_date'] = parse(
                str(scraped_item['content'])).astimezone(TIMEZONE)
            break

    return scraped_result, scraped_pattern


if __name__ == '__main__':
    '''単体テスト用の設定'''
    test_url = 'https://www.asahi.com/articles/ASQ5P42N4Q5PUCVL005.html'
    # 通常サイト用
    request = requests.get(test_url)
    # JavaScript動作後の取得用。Splashを利用。
    # request = requests.get('http://localhost:8050/render.html',
    #                       params={'url': test_url, 'wait': 0.5})
    # bs4で解析
    soup: bs4 = bs4(request.text, 'lxml')

    # scrape用のパラメータ
    # DB内のデータイメージ
    # {'domain': 'yomiuri.co.jp',
    #  'scrape_item': {
    #      'publish_date_scraper': [
    #          {'pattern': 2,
    #           'css_selecter': 'head > meta[property="article:modified_time"]'},
    #          {'pattern': 1,
    #           'css_selecter': 'head > meta[property="article:published_time"]'},
    #      ]}}

    scrape_parm = [{
        "pattern": 1,
        "css_selecter": "head > meta[name=\"pubdate\"]",
    },]
    scrape_parm = sorted(scrape_parm, key=lambda d: d['pattern'], reverse=True)
    print('\n\n=== scrape_parm ===', scrape_parm)

    result = scraper(
        soup=soup,
        scraper='publish_scraper',
        scrape_parm=scrape_parm,
    )
    print('\n\n=== result ===', result)
