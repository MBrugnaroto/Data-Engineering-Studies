import pandas as pd
from bs4 import BeautifulSoup as Soup
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from typing import Dict, List
from urllib.request import urlretrieve


DictType = Dict[str, str]


def scrapeAllPages(url: str, headers: DictType) -> None:
    html: Soup = getHTML(url, headers)
    last_page: int = getTotalPages(html)
    info_cars: List[DictType] = []

    for page in range(last_page):
        html = nextPage(url, page, headers)
        info_cars = [*info_cars, *getInfosFromDiv(html)]

    exportToCSV(info_cars, './output/data/', 'dataset')


def getHTML(url: str, headers: DictType) -> Soup:
    try:
        response = Request(url, headers=headers)
        html = urlopen(response).read()
    except HTTPError as e:
        raise Exception(f'{e.reason}')
    except URLError as e:
        raise Exception(f'{e.reason}')
    return responseHendling(html)


def responseHendling(html: bytes) -> Soup:
    html_decoded = html.decode('utf-8')
    formatted_html = " ".join(html_decoded.split()).replace('> <', '><')
    soup = Soup(formatted_html, 'html.parser')
    return soup


def getTotalPages(html: Soup) -> int:
    info_pages: List[str] = html.find('span',
                                      class_='info-pages').getText().split(' ')
    return int(info_pages[-1])


def nextPage(url: str, page: int, headers: DictType) -> Soup:
    new_page = url+f'?page={page+1}'
    return getHTML(new_page, headers)


def getInfosFromDiv(html: Soup) -> List[DictType]:
    list_cars: List[DictType] = []

    for tag in getAllOcurrOfTheTag(html, 'div', attrs={'class': 'well card'}):
        car_infos: DictType = {}
        p_tags: List[Soup] = tag.findAll('p', text=True)

        for p_tag in p_tags:
            car_infos[getFirstClassName(p_tag)] = p_tag.get_text()

        car_infos['items'] = getValuesFromLI(tag)
        downloadImageFromTag(tag)
        list_cars.append(car_infos)

    return list_cars


def getAllOcurrOfTheTag(html: Soup, tag: str, attrs: DictType) -> List[Soup]:
    return html.findAll(tag, attrs=attrs)


def getFirstClassName(tag: Soup) -> str:
    return tag.get('class')[0].split('-')[-1]


def downloadImageFromTag(tag: Soup) -> None:
    image_url = tag.img.get('src')
    image_name = image_url.split('/')[-1]
    urlretrieve(image_url, './output/img/' + image_name)


def getValuesFromLI(tag: Soup) -> str:
    ul_tag: List[str] = [value.get_text().replace('► ', '') for
                         value in tag.find('ul').findAll('li')]
    ul_tag.pop()
    return " | ".join(ul_tag)


def exportToCSV(data, path, filename) -> None:
    return pd.DataFrame(data).to_csv(
                                    path+filename+'.csv',
                                    sep=';',
                                    index=False,
                                    encoding='utf-8-sig')


def getFirstTagValue(html: Soup, tag: str) -> str:
    return html.find(tag).get_text()


def getFirstTagAttr(html: Soup, tag: str) -> DictType:
    return html.find(tag).attrs


if __name__ == '__main__':
    url = 'https://alura-site-scraping.herokuapp.com/index.php'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
               'AppleWebKit/537.36 (KHTML, like Gecko) ' +
               'Chrome/76.0.3809.100 Safari/537.36'}

    scrapeAllPages(url, headers)
