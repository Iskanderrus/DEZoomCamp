import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess


class ImdbSpiderSpider(CrawlSpider):
    name = "imdb_spider"
    allowed_domains = ["www.imdb.com"]

    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"

    def start_requests(self):
        yield scrapy.Request(url="https://www.imdb.com/feature/genre/?ref_=nv_ch_gr",
                             headers={"User_Agent": self.user_agent})

    rules = (
        Rule(
            LinkExtractor(restrict_xpaths=('//div[@class="image"]/a')),
            follow=True,
            process_request="set_user_agent"
        ),
        # Rule(
        #     LinkExtractor(restrict_xpaths=('//h3/a')),
        #     follow=True,
        #     process_request="set_user_agent"
        # ),
        Rule(
            LinkExtractor(restrict_xpaths='//a[@class="lister-page-next next-page"]'),
            follow=True,
            callback='parse_item',
            process_request="set_user_agent"
        ),
    )

    def set_user_agent(self, request, response):
        request.headers["User-Agent"] = self.user_agent
        return request

    def parse_item(self, response):
        yield {
            "title": response.xpath('//h3/a/text()').get(),
            "year": response.xpath('//span[@class="lister-item-year text-muted unbold"]/text()').get(),
            "rating": response.xpath('//div[@class="inline-block ratings-imdb-rating"]/strong/text()').get(),
            "age": response.xpath('//span[@class="certificate"]/text()').get(),
            "duration": response.xpath('//span[@class="runtime"]/text()').get(),
            "genre": response.xpath('normalize-space(//span[@class="genre"]/text())').get(),
            "votes": response.xpath('//p[@class="sort-num_votes-visible"]/span[@name="nv"][1]/text()').get(),
            "description": response.xpath('normalize-space(//p[@class="text-muted"]/text())').get(),
            "movie_url": response.urljoin(response.xpath('//h3/a/@href').get()),
        }


process = CrawlerProcess(settings={
    'FEED_URI': './data/films.csv',
    'FEED_FORMAT': 'csv'
})

process.crawl(ImdbSpiderSpider)
process.start()
