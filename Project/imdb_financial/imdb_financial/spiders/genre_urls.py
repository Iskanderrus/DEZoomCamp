import scrapy


class GenreUrlsSpider(scrapy.Spider):
    name = "genre_urls"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/search/keyword/?sort=moviemeter,asc&mode=detail&page=1"]

    def parse(self, response):
        yield {
            'genre': response.xpath('//div[@class="table-cell primary"]/a/text()'),
            'genre_url': response.xpath('//div[@class="table-cell primary"]/a/@href')
        }
