import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from time import sleep

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
        sleep(5)
        yield {
            "popularity": product.xpath('.//h3[@class="lister-item-header"]/span[1]/text()').get(),
                "title": product.xpath('.//h3/a[1]/text()').get(),
                "episode": product.xpath('.//h3/a[2]/text()').get(),
                "year": product.xpath('.//span[@class="lister-item-year text-muted unbold"]/text()').get(),
                "episode_year": product.xpath('.//span[@class="lister-item-year text-muted unbold"][2]/text()').get(),
                "rating": product.xpath('.//div[@class="inline-block ratings-imdb-rating"]/strong/text()').get(),
                "age": product.xpath('.//span[@class="certificate"]/text()').get(),
                "duration": product.xpath('.//span[@class="runtime"]/text()').get(),
                "genre": product.xpath('normalize-space(.//span[@class="genre"]/text())').get(),
                "votes": product.xpath('.//p[@class="sort-num_votes-visible"]/span[@name="nv"][1]/text()').get(),
                "description": product.xpath('normalize-space(.//p[@class="text-muted"]/text())').get(),
                "movie_url": response.urljoin(product.xpath('.//h3/a/@href').get()),
        }


process = CrawlerProcess(settings={
    'FEED_URI': './data/films.csv',
    'FEED_FORMAT': 'csv'
})

process.crawl(ImdbSpiderSpider)
process.start()
