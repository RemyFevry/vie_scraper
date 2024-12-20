import logging
import os
import re
from playwright.sync_api import sync_playwright
import agentql 
from pyairtable import Api
from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod
import json
from pathlib import Path

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

INITIAL_URL = "https://mon-vie-via.businessfrance.fr/en/offres/recherche?latest=true"
RAW_INITIAL_URL = "https://mon-vie-via.businessfrance.fr"
MORE_OFFER_BTN_QUERY = """
{
    more_offer_btn
}
"""
JOB_CARD_QUERY = """
{
    job_details{
    nb_candidates(int)
    nb_employees(int)
    nb_countries(int)
        views(int)
        company
        company_description
        company_sector(Summarize in one word the Company Sector)
        country(iso_format)
        city
        state
        title
        contract_type
        salary(float)
        mission
        duration(int)
        from(convert date to YYYY-MM-DD)
        to(convert date to YYYY-MM-DD)
        reference
        publish_date(convert date to YYYY-MM-DD)
        expiry_date(convert date to YYYY-MM-DD)
        perfect_profile
        required_education(Summarize in few word upper case,separated by comma)
        tools(Summarize in few word upper case,separated by comma)
        skills(Summarize in few word upper case,separated by comma)
        experience(Summarize in few word upper case,separated by comma)
        experience_in_years(int)
        languages_needed(list of the languages necesseray for the job upper case,separated by comma)
        contact
        
        
    }
}
"""

JOB_LINKS_QUERY = """
{
    job_posts[]{

        nb_candidates(int)
        views(int)
        
    }
}
"""
LINKS_QUERY = """
{
    job_posts_link[]{
        
    }
}
"""
class AirtableStorage:
    def __init__(self, api_key: str, base_id: str, table_name: str):
        self.api = Api(api_key)
        self.table = self.api.table(base_id, table_name)
    
    def save(self, data: dict):
        # Check if URL already exists
        return self.table.create(data)

NB_CLICKS = 300

@dataclass
class ScraperMetrics:
    airtable_storage: AirtableStorage
    start_time: datetime = field(default_factory=datetime.now)
    jobs_processed: int = 0
    jobs_failed: int = 0
    total_requests: int = 0
    successful_saves: int = 0
    failed_saves: int = 0
     
    
    def to_dict(self):
        duration = (datetime.now() - self.start_time).seconds
        success_rate = self.jobs_processed / (self.jobs_processed + self.jobs_failed) if (self.jobs_processed + self.jobs_failed) > 0 else 0
        return {
            'duration_seconds': duration,
            'jobs_processed': self.jobs_processed,
            'jobs_failed': self.jobs_failed,
            'total_requests': self.total_requests,
            'success_rate': f"{success_rate:.2%}",
            'successful_saves': self.successful_saves,
            'failed_saves': self.failed_saves
        }
    
    def save_metrics(self, filename="scraper_metrics.json"):
        """Save metrics to a airtable"""
        self.airtable_storage.save(self.to_dict())

# Add this before the try block
storage_vie = AirtableStorage(
    api_key=os.getenv("AIRTABLE_API_KEY"),
    base_id=os.getenv("AIRTABLE_BASE_ID"),
    table_name=os.getenv("AIRTABLE_TABLE_NAME")
)
storage_vie_metrics = AirtableStorage(
    api_key=os.getenv("AIRTABLE_API_KEY"),
    base_id=os.getenv("AIRTABLE_BASE_ID"),
    table_name=os.getenv("AIRTABLE_TABLE_NAME_METRICS")
)


try:
    
    
    with sync_playwright() as playwright, playwright.chromium.launch(headless=False,timeout=10000) as browser:
        logger.info("Starting browser session")
        page = agentql.wrap(browser.new_page())
        metrics = ScraperMetrics(airtable_storage=storage_vie_metrics)
        # Login process
        logger.info("Attempting to login")
        page.goto(INITIAL_URL)
        page.get_by_role('link',name="Sign in").click()
        page.wait_for_page_ready_state("complete")
        page.get_by_placeholder("Email Address").fill(os.getenv("EMAIL_ADDRESS"))
        page.get_by_placeholder("Password").fill(os.getenv("EMAIL_PASSWORD"))
        page.get_by_role('button',name="Sign in").click()
        page.wait_for_page_ready_state("complete")
        logger.info("Login successful")

        # Load more results
        click_nb = 0
        logger.info("Starting to load more results")
        stay_in_position = 0
        while click_nb < NB_CLICKS:
            try:
                more_btn = page.get_by_text("Show more offers")
                old_position_btn = more_btn.bounding_box()["y"]
                more_btn.click()
                click_nb += 1
                logger.info(f"Clicked 'Show more' button {click_nb} times")
                page.wait_for_page_ready_state("complete")
                new_position_btn = more_btn.bounding_box()["y"]
                # if new_position_btn == old_position_btn:
                #     stay_in_position+=1
                #     if stay_in_position > 10:
                #         logger.info("No more results to load")
                #         break
                # else:
                #     stay_in_position = 0
            except Exception as e:
                logger.error(f"Error loading more results: {str(e)}")
                break
            

        # Get all job links
        page.wait_for_timeout(3000)
        links = page.get_by_role('link',name="Show Offer").all()
        links = [link.get_attribute('href') for link in links]
        logger.info(f"Found {len(links)} job links to process")

        # Process each job posting
        for i, link in enumerate(links, 1):
            try:
                metrics.total_requests += 1
                full_url = RAW_INITIAL_URL + link
                logger.info(f"Processing job {i}/{len(links)}: {full_url}")
                
                # Check if URL already exists before scraping
                existing = storage_vie.table.all(formula=f"url = '{full_url}'")
                if existing:
                    logger.info(f"URL already exists in Airtable: {full_url}")
                    continue
                
                page.goto(full_url)
                page.wait_for_page_ready_state("complete")
                
                response = page.query_data(JOB_CARD_QUERY, mode='fast')
                response = response.get('job_details')
                response['url'] = full_url
                if response:
                    logger.info(f"Successfully scraped job details for {full_url}")
                    try:
                        storage_vie.save(response)
                        metrics.successful_saves += 1
                    except Exception as e:
                        metrics.failed_saves += 1
                        logger.error(f"Failed to save to Airtable: {str(e)}")
                    metrics.jobs_processed += 1
                else:
                    logger.warning(f"No job details found for {full_url}")
                    metrics.jobs_failed += 1
                
            except Exception as e:
                metrics.jobs_failed += 1
                logger.error(f"Error processing job {link}: {str(e)}")
                continue

        logger.info("Scraping completed successfully")
        
        # Save metrics at the end
        metrics.save_metrics()
        logger.info("Final metrics:", extra=metrics.to_dict())

except Exception as e:
    logger.error(f"Fatal error in main script: {str(e)}")
    if 'metrics' in locals():
        metrics.save_metrics()  # Save metrics even if script fails
    raise




    
