import lzma
import secbulkdownload as sec
from faker import Faker
import datetime
import time
import os


fake = Faker()
def get_random_user_agent() -> str:
    return f"{fake.first_name()} {fake.last_name()} {fake.email()}"
headers = { "User-Agent": f"{get_random_user_agent()}"}

current_year = datetime.datetime.now().year
current_quarter = (datetime.datetime.today().month - 1) // 3 + 1
previous_year = current_year - 1
previous_year_quarter = current_quarter


def absoluteFilePaths(directory, ext=".txt.xz"):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            if f.endswith(ext):
                yield os.path.abspath(os.path.join(dirpath, f))



def filings_13f_download(
    download_dir,
    sy=previous_year,
    sq=previous_year_quarter,
    ey=current_year,
    eq=current_quarter,
):
    """download_dir is directory to download filings into"""
    # Define a user-agent as per SEC developer guidelines.
    start = time.time()
    total_files_before_dl = list(absoluteFilePaths(download_dir))
        # print(f"Total # of files BEFORE download: {len(total_files_before_dl)}")
    
    # Instantiate the idx class
    filings_download = sec.idx(
        start_year=sy,
        start_quarter=sq,
        end_year=ey,
        end_quarter=eq,
        datadir=download_dir,
        user_agent=get_random_user_agent(),
    )
    # # Specify filters as desired
    filters_13f = {"Form Type": ["13F-HR", "13F-HR/A"],
    "CIK": ["1000097", '1336528', '4977', '886982'],
    }

    # # Apply filters to populate idx.working_idx dataframe with filtered index records.
    filings_download.filter_index(filters_13f)

    # print(f"Downloading for period from: {sy}, {sq} to {ey}, {eq}")

    filings_download.fetch_filings(verbose=False)

    filings_13f_download.new_files = filings_download.working_idx.shape[0] - len(
        filings_download.filinglist
    )

    end = time.time()
    # print(f"Step 1 finished in {end - start} seconds")P
    total_files_after_dl = list(absoluteFilePaths(download_dir))
    # print(f"Total # of files AFTER download: {len(total_files_after_dl)}")
    # print(f"# downloaded files: {filings_13f_download.new_files}")
    filings_13f_download.newly_downloaded_filings = set(total_files_after_dl) - set(
        total_files_before_dl
    )






if __name__ == "__main__":
    directory = 'data'
    filings_13f_download(directory, sy=2023, sq=1)




