import json
from bs4 import BeautifulSoup as Soup
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from typing import Dict, List


DictType = Dict[str, str]


def getHTML(url: str, headers: DictType) -> Soup:
    return responseHendling(getResponse(url, headers))


def getResponse(url: str, headers: DictType) -> bytes:
    try:
        response = Request(url, headers=headers)
        html = urlopen(response).read()
    except HTTPError as e:
        raise Exception(f'{e.reason}')
    except URLError as e:
        raise Exception(f'{e.reason}')
    return html


def responseHendling(html: bytes) -> Soup:
    html_decoded = html.decode('utf-8')
    formatted_html = " ".join(html_decoded.split()).replace('> <', '><')
    soup = Soup(formatted_html, 'html.parser')
    return soup


def getFirstTagValue(html: Soup, tag: str) -> str:
    return html.find(tag).get_text()


def getFirstTagAttr(html: Soup, tag: str) -> DictType:
    return html.find(tag).attrs


def getAllOcurrOfTheTag(html: Soup, tag: str, attrs: DictType) -> List[str]:
    return html.findAll(tag, attrs=attrs)


def getInfosFromDiv(html: Soup, attrs: DictType) -> None:
    for tag in getAllOcurrOfTheTag(html, 'div', attrs):
        car_infos: DictType = {}
        all_tags: List[Soup] = tag.findAll('p', text=True)

        for p_tag in all_tags:
            tag_name = p_tag.get('class')[0]
            car_infos[tag_name] = p_tag.get_text()

        car_infos['URL'] = tag.find('img').get('src')

        with open('cars_info.json', 'a+', encoding='utf8') as f:
            f.write(json.dumps(
                car_infos.copy(),
                indent=2,
                ensure_ascii=False)+'\n')


if __name__ == '__main__':
    url = 'https://alura-site-scraping.herokuapp.com/index.php'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
               'AppleWebKit/537.36 (KHTML, like Gecko) ' +
               'Chrome/76.0.3809.100 Safari/537.36'}

    attrs = {
        'class': 'well card'
    }

    html = getHTML(url, headers=headers)
    print(f'Tag value: {getFirstTagValue(html, "h1")} | ' +
          f'Attrs: {getFirstTagAttr(html, "h1")}')

    getInfosFromDiv(html, attrs)
