import os
import re
import logging
import datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import pandas as pd
from github import Github

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("archive_job.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class JobArchiver:
    def __init__(self, github_token):
        self.github_token = github_token
        self.archive_threshold_months = 3
        
    def extract_table_to_dataframe(self, table_content):
        """Convert a markdown table to a pandas DataFrame"""
        try:
            # Split the table into lines
            lines = table_content.strip().split('\n')
            
            # Find the header and separator lines
            header_line = lines[0]
            separator_line = lines[1]
            
            # Extract column names
            headers = [col.strip() for col in header_line.split('|')[1:-1]]
            
            # Create an empty DataFrame
            df = pd.DataFrame(columns=headers)
            
            # Process data rows
            for line in lines[2:]:
                if '|' in line:  # Ensure it's a table row
                    values = [cell.strip() for cell in line.split('|')[1:-1]]
                    if len(values) == len(headers):
                        df.loc[len(df)] = values
            
            return df
        except Exception as e:
            logger.error(f"Failed to parse table: {str(e)}")
            return pd.DataFrame()
    
    def dataframe_to_markdown(self, df):
        """Convert a DataFrame back to markdown table format"""
        if df.empty:
            return "| No entries found |"
        
        # Create the header row
        header = "| " + " | ".join(df.columns) + " |"
        
        # Create the separator row
        separator = "| " + " | ".join(["---"] * len(df.columns)) + " |"
        
        # Create the data rows
        rows = []
        for _, row in df.iterrows():
            rows.append("| " + " | ".join(str(cell) for cell in row) + " |")
        
        # Combine everything
        return "\n".join([header, separator] + rows)
    
    def check_if_outdated(self, date_str):
        """Check if a job posting is older than the threshold"""
        try:
            # Handle different date formats
            if date_str.lower() in ['unknown', 'n/a', '']:
                return True  # Archive entries with unknown dates
            
            # Try to parse the date
            try:
                # First try with explicit format
                if len(date_str) <= 7:  # Format like "2024-03"
                    job_date = datetime.datetime.strptime(date_str, "%Y-%m")
                else:
                    job_date = parse(date_str)
            except:
                # If that fails, try with dateutil parser
                job_date = parse(date_str, fuzzy=True)
            
            # Calculate the difference in months
            current_date = datetime.datetime.now()
            diff_months = relativedelta(current_date, job_date).months + \
                          (12 * relativedelta(current_date, job_date).years)
            
            return diff_months >= self.archive_threshold_months
            
        except Exception as e:
            logger.warning(f"Could not parse date '{date_str}': {str(e)}")
            return False  # Be conservative on failure
    
    def archive_old_jobs(self):
        """Archive job postings older than the threshold"""
        try:
            # Connect to GitHub
            g = Github(self.github_token)
            repo = g.get_repo("izzatkarimov/swe-positions-europe")
            
            # Get the existing README content
            readme_file = repo.get_contents("README.md")
            readme_content = readme_file.decoded_content.decode('utf-8')
            
            # Process Full-Time Jobs section
            jobs_match = re.search(r'## 💼 Full-Time Jobs\s+((?:\|.*\|\s+)+)', readme_content, re.DOTALL)
            if jobs_match:
                jobs_table = jobs_match.group(1)
                jobs_df = self.extract_table_to_dataframe(jobs_table)
                
                if not jobs_df.empty and 'Last Updated' in jobs_df.columns:
                    # Filter out old jobs
                    outdated_mask = jobs_df['Last Updated'].apply(self.check_if_outdated)
                    current_jobs_df = jobs_df[~outdated_mask].reset_index(drop=True)
                    
                    # Count removed jobs
                    removed_jobs_count = len(jobs_df) - len(current_jobs_df)
                    
                    # Update the README with filtered jobs
                    new_jobs_table = self.dataframe_to_markdown(current_jobs_df)
                    new_jobs_section = f"## 💼 Full-Time Jobs\n\n{new_jobs_table}\n"
                    readme_content = re.sub(r'## 💼 Full-Time Jobs\s+((?:\|.*\|\s+)+)', 
                                           new_jobs_section, readme_content, flags=re.DOTALL)
                    
                    logger.info(f"Removed {removed_jobs_count} outdated full-time job postings")
            
            # Process Internships section
            internships_match = re.search(r'## 🚀 Internships\s+((?:\|.*\|\s+)+)', readme_content, re.DOTALL)
            if internships_match:
                internships_table = internships_match.group(1)
                internships_df = self.extract_table_to_dataframe(internships_table)
                
                if not internships_df.empty and 'Last Updated' in internships_df.columns:
                    # Filter out old internships
                    outdated_mask = internships_df['Last Updated'].apply(self.check_if_outdated)
                    current_internships_df = internships_df[~outdated_mask].reset_index(drop=True)
                    
                    # Count removed internships
                    removed_internships_count = len(internships_df) - len(current_internships_df)
                    
                    # Update the README with filtered internships
                    new_internships_table = self.dataframe_to_markdown(current_internships_df)
                    new_internships_section = f"## 🚀 Internships\n\n{new_internships_table}\n"
                    readme_content = re.sub(r'## 🚀 Internships\s+((?:\|.*\|\s+)+)', 
                                           new_internships_section, readme_content, flags=re.DOTALL)
                    
                    logger.info(f"Removed {removed_internships_count} outdated internship postings")
            
            # Update the archive date
            archive_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if "Last Archived:" in readme_content:
                readme_content = re.sub(r'Last Archived:.*', f'Last Archived: {archive_date}', readme_content)
            else:
                # Add archive date after Last Updated
                readme_content = re.sub(r'(Last Updated:.*)', 
                                       f'\\1\nLast Archived: {archive_date}', readme_content)
            
            # Update the README in the repository
            repo.update_file(
                "README.md",
                f"Archive outdated job listings - {archive_date}",
                readme_content,
                readme_file.sha
            )
            
            logger.info(f"Successfully archived outdated job postings at {archive_date}")
            
            return {
                "status": "success",
                "archive_date": archive_date,
                "full_time_jobs_removed": removed_jobs_count if 'removed_jobs_count' in locals() else 0,
                "internships_removed": removed_internships_count if 'removed_internships_count' in locals() else 0,
            }
            
        except Exception as e:
            logger.error(f"Error archiving outdated job postings: {str(e)}")
            return {"status": "error", "message": str(e)}

def main():
    # Get GitHub token from environment variable
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        logger.error("GitHub token not found. Please set the GITHUB_TOKEN environment variable.")
        return
    
    # Initialize the archiver
    archiver = JobArchiver(github_token)
    
    try:
        # Archive old job postings
        result = archiver.archive_old_jobs()
        logger.info(f"Archive job result: {result}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()