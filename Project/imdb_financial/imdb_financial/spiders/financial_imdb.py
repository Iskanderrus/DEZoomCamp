import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess


class FinancialImdbSpider(CrawlSpider):
    name = 'financial_imdb'
    allowed_domains = ['www.imdb.com']
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"

    def start_requests(self):
        yield scrapy.Request(url="https://www.imdb.com/feature/genre/?ref_=nv_ch_gr", headers={"User_Agent": self.user_agent})

    rules = (
        Rule(
            LinkExtractor(restrict_xpaths=('//div[@class="image"]/a')),
            follow=True,
            process_request="set_user_agent"
        ),
        Rule(
            LinkExtractor(restrict_xpaths=('//h3/a')),
            follow=True,
            callback='parse_item',
            process_request="set_user_agent"
        ),
        Rule(
            LinkExtractor(restrict_xpaths=('(//a[@class="lister-page-next next-page"])[2]')),
            follow=True,
            process_request="set_user_agent"
        ),
    )

    def set_user_agent(self, request, response):
        request.headers["User-Agent"] = self.user_agent
        return request


    def parse_item(self, response):
        yield {
            "title": response.xpath('//h1/span/text()').get(),
            "country_of_origin": response.xpath('//div[@data-testid="title-details-section"]/ul/li[2]//a/text()').get(),
            "production_company": response.xpath(
                '//div[@data-testid="title-details-section"]/ul/li[6]//ul/li/a/text()').getall(),
            "budget_local_currency": response.xpath('(//div[@data-testid="title-boxoffice-section"]//div/ul/li/span/text())[1]').get(),
            "gross_us_canada": response.xpath('(//div[@data-testid="title-boxoffice-section"]//div/ul/li/span/text())[2]').get(),
            "opening_weekend_us_canada": response.xpath('(//section[@data-testid="BoxOffice"]//li[@role="presentation"]/div/ul/li[@role="presentation"]/span)[3]').get(),
            "opening_weekend_date": response.xpath('(//section[@data-testid="BoxOffice"]//li[@role="presentation"]/div/ul/li[@role="presentation"]/span)[4]').get(),
            "movie_url": response.urljoin(response.xpath('//h3/a/@href').get()),
        }

process = CrawlerProcess(settings={
    'FEED_URI': './data/films_financials.csv', 
    'FEED_FORMAT': 'csv'
})

process.crawl(FinancialImdbSpider)
process.start()