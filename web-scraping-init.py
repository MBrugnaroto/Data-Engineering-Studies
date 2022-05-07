from bs4 import BeautifulSoup as Soup
from urllib.request import urlopen as URLOpen


def returnH1Value(url: str) -> None:
    response = URLOpen(url)
    html = response.read()

    formatted_html = Soup(html, 'html.parser')
    result = formatted_html.find('h1', {'class': 'sub-header'}).getText()

    print(f'{result}')


if __name__ == '__main__':
    url = 'https://alura-site-scraping.herokuapp.com/hello-world.php'

    returnH1Value(url)
