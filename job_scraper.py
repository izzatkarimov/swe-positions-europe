import os
import time
import random
import re
import json
import logging
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from github import Github

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("job_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class JobScraper:
    def __init__(self, github_token):
        self.github_token = github_token
        self.setup_selenium()
        self.jobs_data = []
        self.internships_data = []
        
    def setup_selenium(self):
        """Set up Selenium with Chrome in headless mode"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        
    def random_delay(self, min_seconds=1, max_seconds=3):
        """Add random delay to avoid detection"""
        time.sleep(random.uniform(min_seconds, max_seconds))
        
    def scrape_linkedin(self, keywords, locations, days_ago=7):
        """Scrape jobs from LinkedIn"""
        logger.info("Starting LinkedIn scraping...")
        
        for location in locations:
            for keyword in keywords:
                url = f"https://www.linkedin.com/jobs/search/?keywords={keyword}&location={location}&f_TPR=r{days_ago}d"
                logger.info(f"Scraping LinkedIn with URL: {url}")
                
                try:
                    self.driver.get(url)
                    self.random_delay(2, 4)
                    
                    # Scroll to load more jobs
                    for i in range(5):
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        self.random_delay()
                    
                    job_cards = self.driver.find_elements(By.CLASS_NAME, "job-search-card")
                    
                    for card in job_cards:
                        try:
                            title_element = card.find_element(By.CLASS_NAME, "base-search-card__title")
                            company_element = card.find_element(By.CLASS_NAME, "base-search-card__subtitle")
                            location_element = card.find_element(By.CLASS_NAME, "job-search-card__location")
                            link_element = card.find_element(By.CSS_SELECTOR, "a.base-card__full-link")
                            
                            title = title_element.text.strip()
                            company = company_element.text.strip()
                            location = location_element.text.strip()
                            link = link_element.get_attribute("href")
                            
                            # Determine if it's an internship
                            is_internship = any(keyword.lower() in title.lower() for keyword in ["intern", "internship", "trainee"])
                            
                            # Default to unknown work mode
                            work_mode = self.determine_work_mode(title, "Unknown")
                            
                            job_data = {
                                "company": company,
                                "role": title,
                                "work_mode": work_mode,
                                "location": location,
                                "link": link,
                                "source": "LinkedIn"
                            }
                            
                            if is_internship:
                                self.internships_data.append(job_data)
                            else:
                                self.jobs_data.append(job_data)
                                
                        except NoSuchElementException as e:
                            logger.error(f"Error extracting job details: {str(e)}")
                            continue
                
                except Exception as e:
                    logger.error(f"Error scraping LinkedIn: {str(e)}")
                    
                self.random_delay(5, 10)  # Longer delay between searches
                
    def scrape_indeed(self, keywords, locations, days_ago=7):
        """Scrape jobs from Indeed"""
        logger.info("Starting Indeed scraping...")
        
        for location in locations:
            for keyword in keywords:
                url = f"https://www.indeed.com/jobs?q={keyword}&l={location}&fromage={days_ago}"
                logger.info(f"Scraping Indeed with URL: {url}")
                
                try:
                    self.driver.get(url)
                    self.random_delay(2, 4)
                    
                    # Accept cookies if popup appears
                    try:
                        cookie_button = self.wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                        cookie_button.click()
                        self.random_delay()
                    except TimeoutException:
                        pass
                    
                    # Scroll to load more jobs
                    for i in range(3):
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        self.random_delay()
                    
                    job_cards = self.driver.find_elements(By.CSS_SELECTOR, ".job_seen_beacon")
                    
                    for card in job_cards:
                        try:
                            title_element = card.find_element(By.CSS_SELECTOR, ".jobTitle span")
                            company_element = card.find_element(By.CSS_SELECTOR, ".companyName")
                            location_element = card.find_element(By.CSS_SELECTOR, ".companyLocation")
                            
                            title = title_element.text.strip()
                            company = company_element.text.strip()
                            location = location_element.text.strip()
                            
                            # Get job URL
                            job_id = card.get_attribute("data-jk")
                            link = f"https://www.indeed.com/viewjob?jk={job_id}"
                            
                            # Determine if it's an internship
                            is_internship = any(keyword.lower() in title.lower() for keyword in ["intern", "internship", "trainee"])
                            
                            # Look for remote/hybrid indicators
                            try:
                                metadata = card.find_elements(By.CSS_SELECTOR, ".metadata span")
                                work_mode_text = " ".join([span.text for span in metadata])
                                work_mode = self.determine_work_mode(work_mode_text, "Unknown")
                            except:
                                work_mode = self.determine_work_mode(title, "Unknown")
                            
                            job_data = {
                                "company": company,
                                "role": title,
                                "work_mode": work_mode,
                                "location": location,
                                "link": link,
                                "source": "Indeed"
                            }
                            
                            if is_internship:
                                self.internships_data.append(job_data)
                            else:
                                self.jobs_data.append(job_data)
                                
                        except NoSuchElementException as e:
                            logger.error(f"Error extracting Indeed job details: {str(e)}")
                            continue
                
                except Exception as e:
                    logger.error(f"Error scraping Indeed: {str(e)}")
                    
                self.random_delay(5, 10)  # Longer delay between searches
    
    def scrape_glassdoor(self, keywords, locations, days_ago=7):
        """Scrape jobs from Glassdoor"""
        logger.info("Starting Glassdoor scraping...")
        
        for location in locations:
            for keyword in keywords:
                url = f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={keyword}&locT=N&locId=142&fromAge={days_ago}"
                logger.info(f"Scraping Glassdoor with URL: {url}")
                
                try:
                    self.driver.get(url)
                    self.random_delay(3, 5)
                    
                    # Handle sign-in popup if it appears
                    try:
                        close_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "[alt='Close']")))
                        close_button.click()
                        self.random_delay()
                    except TimeoutException:
                        pass
                    
                    # Scroll to load more jobs
                    for i in range(3):
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        self.random_delay(1, 2)
                    
                    job_cards = self.driver.find_elements(By.CSS_SELECTOR, ".react-job-listing")
                    
                    for card in job_cards:
                        try:
                            # Click on the card to load job details
                            card.click()
                            self.random_delay(1, 2)
                            
                            title_element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".job-title")))
                            company_element = self.driver.find_element(By.CSS_SELECTOR, ".employer-name")
                            location_element = self.driver.find_element(By.CSS_SELECTOR, ".location")
                            
                            title = title_element.text.strip()
                            company = company_element.text.strip()
                            location = location_element.text.strip()
                            
                            # Get the current URL as it should now include the job details
                            link = self.driver.current_url
                            
                            # Look for job type information
                            try:
                                job_details = self.driver.find_element(By.CSS_SELECTOR, ".jobDescriptionContent").text
                                work_mode = self.determine_work_mode(job_details, "Unknown")
                            except:
                                work_mode = self.determine_work_mode(title, "Unknown")
                            
                            # Determine if it's an internship
                            is_internship = any(keyword.lower() in title.lower() for keyword in ["intern", "internship", "trainee"])
                            
                            job_data = {
                                "company": company,
                                "role": title,
                                "work_mode": work_mode,
                                "location": location,
                                "link": link,
                                "source": "Glassdoor"
                            }
                            
                            if is_internship:
                                self.internships_data.append(job_data)
                            else:
                                self.jobs_data.append(job_data)
                                
                        except (NoSuchElementException, TimeoutException) as e:
                            logger.error(f"Error extracting Glassdoor job details: {str(e)}")
                            continue
                
                except Exception as e:
                    logger.error(f"Error scraping Glassdoor: {str(e)}")
                    
                self.random_delay(5, 10)  # Longer delay between searches
    
    def determine_work_mode(self, text, default="Unknown"):
        """Determine the work mode based on job description or title"""
        text = text.lower()
        
        if re.search(r'\bremote\b|\bfully remote\b', text):
            return "Remote"
        elif re.search(r'\bon[- ]?site\b|\bin[- ]?office\b', text):
            return "On-site"
        elif re.search(r'\bhybrid\b', text):
            return "Hybrid"
        else:
            return default
    
def format_markdown_tables(self):
    """Format the collected job data into markdown tables"""
    # Get current date in YYYY-MM format
    current_date = datetime.datetime.now().strftime("%Y-%m")
    
    # Create dataframes
    if self.jobs_data:
        # Add current date to all entries
        for job in self.jobs_data:
            job['last_updated'] = current_date
            
        jobs_df = pd.DataFrame(self.jobs_data)
        jobs_df = jobs_df.drop_duplicates(subset=['company', 'role', 'location']).reset_index(drop=True)
        
        # Rename columns to match README format
        jobs_df = jobs_df.rename(columns={
            'company': 'Company',
            'role': 'Role',
            'work_mode': 'Work Mode',
            'location': 'Location',
            'link': 'Link to Application',
            'last_updated': 'Last Updated'
        })
        
        # Reorder columns
        column_order = ['Company', 'Role', 'Work Mode', 'Location', 'Link to Application', 'Last Updated']
        jobs_df = jobs_df[column_order]
    else:
        jobs_df = pd.DataFrame(columns=['Company', 'Role', 'Work Mode', 'Location', 'Link to Application', 'Last Updated'])
    
    if self.internships_data:
        # Add current date to all entries
        for job in self.internships_data:
            job['last_updated'] = current_date
            
        internships_df = pd.DataFrame(self.internships_data)
        internships_df = internships_df.drop_duplicates(subset=['company', 'role', 'location']).reset_index(drop=True)
        
        # Rename columns to match README format
        internships_df = internships_df.rename(columns={
            'company': 'Company',
            'role': 'Role',
            'work_mode': 'Work Mode',
            'location': 'Location',
            'link': 'Link to Application',
            'last_updated': 'Last Updated'
        })
        
        # Add duration column for internships if not present
        if 'Duration' not in internships_df.columns:
            # Try to extract duration from role name or set to default
            internships_df['Duration'] = internships_df['Role'].apply(
                lambda x: 'Summer 2024' if any(term in x.lower() for term in ['summer', 'season']) else 'Ongoing'
            )
        
        # Reorder columns
        column_order = ['Company', 'Role', 'Work Mode', 'Location', 'Link to Application', 'Last Updated', 'Duration']
        internships_df = internships_df[column_order]
    else:
        internships_df = pd.DataFrame(columns=['Company', 'Role', 'Work Mode', 'Location', 'Link to Application', 'Last Updated', 'Duration'])
    
    # Format work_mode column with backticks
    jobs_df['Work Mode'] = jobs_df['Work Mode'].apply(lambda x: f"`{x}`")
    internships_df['Work Mode'] = internships_df['Work Mode'].apply(lambda x: f"`{x}`")
    
    # Format link column to make it clickable in Markdown
    jobs_df['Link to Application'] = jobs_df['Link to Application'].apply(lambda x: f"[Apply]({x})")
    internships_df['Link to Application'] = internships_df['Link to Application'].apply(lambda x: f"[Apply]({x})")
    
    # Format for markdown
    jobs_md = jobs_df.to_markdown(index=False) if not jobs_df.empty else "| No full-time jobs found |"
    internships_md = internships_df.to_markdown(index=False) if not internships_df.empty else "| No internships found |"
    
    return jobs_md, internships_md
    
def update_github_readme(self):
    """Update the GitHub repository README with the new job data"""
    try:
        # Format the job data
        jobs_md, internships_md = self.format_markdown_tables()
        
        # Connect to GitHub
        g = Github(self.github_token)
        repo = g.get_repo("izzatkarimov/EU-Swe-Jobs")
        
        # Get the existing README content
        readme_content = repo.get_contents("README.md").decoded_content.decode('utf-8')
        
        # Check if we have new data to add
        if self.jobs_data:
            # Update the full-time jobs section - note the ## instead of ###
            jobs_pattern = r'## 💼 Full-Time Jobs\s+\|.*?\|\s+(?:\|.*?\|\s+)*?(?=\n##|\Z)'
            new_jobs_section = f'## 💼 Full-Time Jobs\n\n{jobs_md}\n'
            
            if re.search(jobs_pattern, readme_content, flags=re.DOTALL):
                readme_content = re.sub(jobs_pattern, new_jobs_section, readme_content, flags=re.DOTALL)
            else:
                logger.warning("Could not find full-time jobs section in README")
        else:
            logger.info("No new full-time jobs to add")
        
        # Check if we have new internship data
        if self.internships_data:
            # Update the internships section - note the ## instead of ###
            internships_pattern = r'## 🚀 Internships\s+\|.*?\|\s+(?:\|.*?\|\s+)*?(?=\n##|\Z)'
            new_internships_section = f'## 🚀 Internships\n\n{internships_md}\n'
            
            if re.search(internships_pattern, readme_content, flags=re.DOTALL):
                readme_content = re.sub(internships_pattern, new_internships_section, readme_content, flags=re.DOTALL)
            else:
                logger.warning("Could not find internships section in README")
        else:
            logger.info("No new internships to add")
        
        # If we have no data at all, don't update the README
        if not self.jobs_data and not self.internships_data:
            logger.warning("No jobs or internships found. Skipping README update.")
            return {"status": "skipped", "reason": "No data found"}
        
        # Add update timestamp
        update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if "Last Updated:" in readme_content:
            readme_content = re.sub(r'Last Updated:.*', f'Last Updated: {update_time}', readme_content)
        else:
            readme_content = readme_content.replace("# Software Engineer Job Openings in Europe", 
                                                   f"# Software Engineer Job Openings in Europe\n\nLast Updated: {update_time}")
        
        # Update the README in the repository
        repo.update_file(
            "README.md",
            f"Update job listings - {update_time}",
            readme_content,
            repo.get_contents("README.md").sha
        )
        
        logger.info(f"Successfully updated GitHub README at {update_time}")
        
        # Generate a detailed report
        report = {
            "status": "success",
            "update_time": update_time,
            "total_full_time_jobs": len(self.jobs_data),
            "total_internships": len(self.internships_data),
            "sources": {
                "LinkedIn": len([j for j in self.jobs_data + self.internships_data if j["source"] == "LinkedIn"]),
                "Indeed": len([j for j in self.jobs_data + self.internships_data if j["source"] == "Indeed"]),
                "Glassdoor": len([j for j in self.jobs_data + self.internships_data if j["source"] == "Glassdoor"])
            }
        }
        
        return report
        
    except Exception as e:
        logger.error(f"Error updating GitHub repository: {str(e)}")
        return {"status": "error", "message": str(e)}
    
    def close(self):
        """Close the Selenium driver"""
        if hasattr(self, 'driver'):
            self.driver.quit()
            logger.info("Selenium driver closed")

    def deduplicate_jobs(self):
        """Remove duplicate job postings based on multiple criteria"""
        def get_job_key(job):
            return f"{job['company']}_{job['role']}_{job['location']}"
        
        seen_jobs = set()
        unique_jobs = []
        
        for job in self.jobs_data:
            job_key = get_job_key(job)
            if job_key not in seen_jobs:
                seen_jobs.add(job_key)
                unique_jobs.append(job)
        
        self.jobs_data = unique_jobs

def main():
    # Get GitHub token from environment variable for security
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        logger.error("GitHub token not found. Please set the GITHUB_TOKEN environment variable.")
        return
    
    # Initialize the scraper
    scraper = JobScraper(github_token)
    
    try:
        # Define search parameters
        keywords = [
            "software engineer", 
            "software developer",
            "frontend developer",
            "backend developer",
            "backend engineer",
            "full stack developer",
            "python developer",
            "javascript developer",
            "java developer",
            "mobile app developer",
            "data scientist",
            "machine learning engineer",
            "devops engineer",
            "quality assurance engineer",
            "technical lead",
            "solutions architect",
            "cloud engineer",
            "cybersecurity engineer",
            "database administrator"
        ]
        
        # European locations
        locations = [
            "Warsaw, Poland",
            "Berlin, Germany",
            "Amsterdam, Netherlands",
            "London, United Kingdom",
            "Paris, France",
            "Madrid, Spain",
            "Stockholm, Sweden",
            "Dublin, Ireland",
            "Zurich, Switzerland",
            "Vienna, Austria",
            "Rome, Italy",
            "Brussels, Belgium",
            "Copenhagen, Denmark",
            "Helsinki, Finland",
            "Oslo, Norway",
            "Prague, Czech Republic",
            "Budapest, Hungary",
            "Sofia, Bulgaria",
            "Athens, Greece",
            "Lisbon, Portugal",
            "Bucharest, Romania",
            "Sarajevo, Bosnia and Herzegovina",
            "Zagreb, Croatia",
            "Ljubljana, Slovenia",
            "Skopje, North Macedonia",
            "Tirana, Albania",
            "Belgrade, Serbia",
            "Podgorica, Montenegro",
            "Prishtina, Kosovo",
            "Chisinau, Moldova",
            "Tallinn, Estonia",
            "Riga, Latvia",
            "Vilnius, Lithuania",
            "Minsk, Belarus",
            "Kiev, Ukraine",
            "Moscow, Russia",
            "Istanbul, Turkey",
            "Nicosia, Cyprus",
            "Valletta, Malta",
            "San Marino, San Marino",
            "Vaduz, Liechtenstein",
            "Monaco, Monaco",
            "Andorra la Vella, Andorra"
        ]
        
        # Scrape job boards
        scraper.scrape_linkedin(keywords, locations)
        scraper.scrape_indeed(keywords, locations)
        scraper.scrape_glassdoor(keywords, locations)
        
        # Update GitHub repository
        report = scraper.update_github_readme()
        
        if report:
            logger.info(f"Job update summary: {json.dumps(report, indent=2)}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()