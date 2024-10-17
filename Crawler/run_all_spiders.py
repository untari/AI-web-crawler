import asyncio
import os
from twisted.internet import asyncioreactor
from scrapy.exceptions import DropItem
import logging

# Set the event loop policy to use SelectorEventLoop, which is compatible with Twisted
if os.name == 'nt':  # Only necessary on Windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

asyncioreactor.install(asyncio.get_event_loop())

# The rest of your imports and code follow here...
from twisted.internet import reactor, defer
from twisted.internet.error import ReactorNotRunning
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings

# Import all your spider classes
from Crawler.spiders.AFSpider import MarketSpiderAFS as AFSpider
from Crawler.spiders.ASTSpider import MarketSpiderAST as ASTSpider
from Crawler.spiders.CATSpider import MarketSpiderCAT as CATSpider
from Crawler.spiders.FBKSpider import MarketSpiderFBK as FBKSpider
from Crawler.spiders.FKZSpider import MarketSpiderFKZ as FKZSpider
from Crawler.spiders.UZASpider import MarketSpiderUZA as UZASpider
from Crawler.spiders.GAZSpider import MarketSpiderGAZ as GAZSpider
from Crawler.spiders.SPTSpider import MarketSpiderSPT as SPTSpider
from Crawler.pipelines import AccumulatePipeline, CrawlerPipeline, ComparePipeline, DraftPipeline
# ... continue importing all your spiders

settings = get_project_settings()
configure_logging(settings)
runner = CrawlerRunner(settings)

def run_spiders():
    """Initializes and runs all spiders concurrently."""
    crawls = [
        # runner.crawl(FBKSpider),
        # runner.crawl(CATSpider), #this website got shut down
        # runner.crawl(ASTSpider),
        # runner.crawl(FKZSpider),
        runner.crawl(AFSpider),
        # runner.crawl(UZASpider),
        # runner.crawl(GAZSpider),
        # runner.crawl(SPTSpider),
        # Add all your spiders here
    ]
    # Wait for all spiders to finish using gatherResults
    d = defer.gatherResults(crawls)
    d.addBoth(lambda _: process_all_items_and_stop())

def process_all_items_and_stop():
    all_items = AccumulatePipeline.get_accumulated_items()
    output_file_path = 'accumulated_items.txt'  # Adjust the path as per your requirement

    try:
        # Attempt to process items through pipelines
        processed_items = process_items_through_pipelines(all_items)  # Assuming this returns processed items
        # Write processed items to the output file
        with open(output_file_path, 'w', encoding='utf-8') as file:
            for item in processed_items:  # Assuming processed_items is the correct list to iterate over
                # Ensure each item is correctly formatted as a string
                item_str = str(item)
                file.write(f"{item_str}\n")
    except DropItem as e:
        logging.error(f"Item dropped due to error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error processing items: {e}")
    finally:
        # Attempt to safely stop the Twisted reactor
        try:
            reactor.stop()
        except ReactorNotRunning:
            logging.warning("Tried to stop an already stopped reactor.")


def process_items_through_pipelines(all_items):
    """Processes all items collected by spiders after crawling is complete."""

     # Initialize and process through CrawlerPipeline
    crawler_pipeline = CrawlerPipeline()
    processed_items = crawler_pipeline.process_item(all_items)
    # Write processed items to a file for inspection
    with open('processed_items.txt', 'w', encoding='utf-8') as f:
        for item in processed_items:
            f.write(f"{item}\n")
    
    # Initialize ComparePipeline and process items if there are any

    if processed_items:
        compare_pipeline = ComparePipeline()
        grouped_articles = compare_pipeline.process_grouped_articles(processed_items)
        # Write grouped articles to a file for inspection
        with open('grouped.txt', 'w', encoding='utf-8') as f:
            for group_id, articles in grouped_articles.items():
                f.write(f"Group ID {group_id}:\n{articles}\n\n")
    else:
        logging.info("No processed items to compare.")
        return []

    # Initialize DraftPipeline and process grouped articles if there are any
    if grouped_articles:
        draft_pipeline = DraftPipeline()
        draft_articles = draft_pipeline.close(grouped_articles)  # This method needs to match your implementation
    else:
        logging.info("No grouped articles to draft.")
        return []

    # Return the final draft articles
    return draft_articles

if __name__ == '__main__':
    reactor.callWhenRunning(run_spiders)
    reactor.run()   # the script will block here until the last crawl call is finished
