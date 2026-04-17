import streamlit as st
import os
import asyncio
import pandas as pd
import aiohttp
import nest_asyncio
import requests
import re

from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import SSLError, RequestException
from yarl import URL
from datetime import datetime
from aiohttp import (
    ClientConnectorError,
    ClientConnectorSSLError,
    ClientResponseError,
    ClientTimeout,
    ServerDisconnectedError
)

nest_asyncio.apply()

BATCH_SIZE = 2500
MAX_RECORDS = 100000
CONCURRENCY = 400
TIMEOUT = 40

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

csv_output_path = f"Output/Final_Results_{timestamp}.csv"

os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)

def ensure_scheme(url):
    try:
        parsed = URL(url)
        if not parsed.scheme:
            return f"https://{url}"
        elif parsed.scheme == "http":
            return str(parsed.with_scheme("https"))
        return url
    except Exception:
        return url

async def dns_resolves(url):
    try:
        host = URL(url).host
        loop = asyncio.get_event_loop()
        await loop.getaddrinfo(host, None)
        return True
    except Exception:
        return False

def normalize_url(url):
    """Return URL without scheme or www prefix for comparison purposes."""
    parsed = URL(url)
    if parsed.host is None:
        return None
    hostname = parsed.host.lower()
    # Remove 'www.' if present
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname.split('.')[0]

def is_valid_hostname(hostname):
    if len(hostname) > 255:
        return False
    if hostname.endswith("."):
        hostname = hostname[:-1]  # Strip trailing dot
    allowed = re.compile(r"(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))

async def check(session, url, retries=3):
    original_url = url
    url = ensure_scheme(url)

    for attempt in range(retries + 1):
        try:
            headers = {
                     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
                     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
            }
            async with session.get(url, timeout=TIMEOUT, allow_redirects=False, headers=headers  ) as response:
                code = response.status
                if 200 <= code < 300:
                    return {"website": original_url, "status": f"Working {code}", "redirect_url": None, "createddate": datetime.now()}
                elif 300 <= code < 400:
                    redirect_url = response.headers.get("Location", "Unknown")
                    if redirect_url == "Unknown":
                        return {
                            "website": original_url,
                            "status": f"Redirected {code}",
                            "redirect_url": redirect_url,
                            "createddate": datetime.now()
                        }

                    # Ensure redirect_url is absolute
                    redirect_url = str(URL(url).join(URL(redirect_url)))
                    normalized_original = normalize_url(url)
                    normalized_redirect = normalize_url(redirect_url)

                    if normalized_original == normalized_redirect:
                        # Only protocol changed (e.g., http -> https), consider working
                        return {
                            "website": original_url,
                            "status": f"Working {code}",
                            "redirect_url": None,
                            "createddate": datetime.now()
                        }
                    else:
                        return {
                            "website": original_url,
                            "status": f"Redirected {code}",
                            "redirect_url": redirect_url,
                            "createddate": datetime.now()
                        }

                else:
                    if code == 503 or code == 500 or code == 522 or code == 520 or code == 436 or code == 530 and retries > 0:
                        await asyncio.sleep(2 ** (retries - 1))
                        continue  # retry

                    if code in {400,402,404,405,406,409,410,410,418,444}:
                        # Definitely non-working: resource missing
                        status = f"Non Working {code}"
                    elif code in {401, 403, 429}:
                        # Restricted access but site reachable
                        status = f"Working but Restricted {code}"
                    elif 500 <= code < 600:
                        # Server errors, mark as non-working
                        status = f"Non Working {code}"
                    else:
                        # Catch-all fallback (treat unknown codes as non-working)
                        status = f"Non Working {code}"
                    return {"website": original_url, "status": status,"redirect_url": None,"createddate": datetime.now()}

        except ClientConnectorSSLError:
            if url.startswith("https://"):
                url = url.replace("https://", "http://", 1)
                continue
        except (asyncio.TimeoutError, ClientConnectorError, ClientResponseError, ServerDisconnectedError) as e:
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
                continue
            return {"website": original_url, "status": f"Not Able to Verify - {type(e).__name__}","redirect_url": None,"createddate": datetime.now()}
        except Exception as e:
            return {"website": original_url, "status": f"Not Able to Verify - {type(e).__name__}", "redirect_url": None, "createddate": datetime.now() }

    return {"website": original_url, "status": "Not Able to Verify - Unknown Error", "redirect_url": None, "createddate": datetime.now()}

def sync_check_not_verified_url_status(url):
    original_url = url

    def try_request(test_url, verify_ssl=True):
        try:
            response = requests.get(test_url, timeout=25, allow_redirects=False, verify=verify_ssl)
            code = response.status_code
            if 200 <= code < 300:
                return {"website": original_url, "status": f"Working {code}", "redirect_url": None, "createddate": datetime.now()}
            elif 300 <= code < 400:
                redirect_url = response.headers.get("Location", "Unknown")
                if redirect_url == "Unknown":
                    return {
                            "website": original_url,
                            "status": f"Redirected {code}",
                            "redirect_url": redirect_url,
                            "createddate": datetime.now()
                    }

                # Ensure redirect_url is absolute
                redirect_url = str(URL(url).join(URL(redirect_url)))
                normalized_original = normalize_url(url)
                normalized_redirect = normalize_url(redirect_url)

                if normalized_original == normalized_redirect:
                    # Only protocol changed (e.g., http -> https), consider working
                    return {
                        "website": original_url,
                        "status": f"Working {code}",
                        "redirect_url": None,
                        "createddate": datetime.now()
                    }
                else:
                    return {
                        "website": original_url,
                        "status": f"Redirected {code}",
                        "redirect_url": redirect_url,
                        "createddate": datetime.now()
                    }
            else:
                # return {"website": original_url, "status": f"Non Working {code}", "redirect_url": None, "createddate": datetime.now()}
                if code in {400,402,404,405,406,409,410,410,418,429,444}:
                    # Definitely non-working: resource missing
                    status = f"Non Working {code}"
                elif code in {401, 403, 429}:
                    # Restricted access but site reachable
                    status = f"Working but Restricted {code}"
                elif 500 <= code < 600:
                    # Server errors, mark as non-working
                    status = f"Non Working {code}"
                else:
                    # Catch-all fallback (treat unknown codes as non-working)
                    status = f"Non Working {code}"
                return {"website": original_url, "status": status,"redirect_url": None,"createddate": datetime.now()}
        except SSLError as e:
            # SSL error happened here, try HTTP fallback if HTTPS
            if test_url.startswith("https://"):
                fallback_url = test_url.replace("https://", "http://", 1)
                #print(f"SSL Error on {test_url}, retrying with HTTP...")
                return try_request(fallback_url)
            else:
                return {"website": original_url, "status": f"Not Able to Verify (Sync) - SSLError", "redirect_url": None, "createddate": datetime.now()}
        except RequestException as e:
            return {"website": original_url, "status": f"Not Able to Verify (Sync) - {type(e).__name__}", "redirect_url": None, "createddate": datetime.now()}

    # Ensure scheme is HTTPS by default
    parsed = urlparse(url)

    if not parsed.scheme:
        url = f"https://{url}"
    elif parsed.scheme == "http":
        url = url.replace("http://", "https://", 1)

    return try_request(url)

async def filter_resolvable_async(records):
    tasks = []
    for row in records:
        url = ensure_scheme(row.website)
        tasks.append(dns_resolves(url))

    results = await asyncio.gather(*tasks)
    return [row for row, is_ok in zip(records, results) if is_ok]


async def process_all(records, concurrency=CONCURRENCY):
    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    timeout = ClientTimeout(total=TIMEOUT, sock_read=20)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [check(session, row.website, retries=3) for row in records]
        return await asyncio.gather(*tasks)


def process_batches(uploaded_file):

    # Read CSV with original encoding
    df_all = pd.read_csv(uploaded_file, encoding='latin-1')

    st.write(f"📊 Total Records Found: **{len(df_all)}**")

    if df_all.empty:
        st.error("No records found in CSV.")
        return None

    # Create batches in memory
    records = list(df_all.itertuples(index=False))
    batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]

    all_results = []

    # UI Progress components
    batch_status = st.empty()
    progress_bar = st.progress(0)

    for i, batch in enumerate(batches, start=1):
        batch_status.markdown(f"**Batch {i}/{len(batches)}**: Processing {len(batch)} records...")

        # Filter DNS-resolvable (Keeping your logic)
        resolvable = asyncio.run(filter_resolvable_async(batch))

        # Check website status
        results = asyncio.run(process_all(resolvable))
        all_results.extend(results)

        progress_bar.progress(i / len(batches))

    # Reprocess items with "Not Able to Verify" status
    not_verified = [r for r in all_results if "Not Able to Verify" in r["status"]]

    if not_verified:
        st.warning(f"⚠️ Performing final fallback for {len(not_verified)} records...")

        def worker(record):
            return sync_check_not_verified_url_status(record["website"])

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(worker, record) for record in not_verified]
            final_sync_results = [future.result() for future in as_completed(futures)]

        # Remove old 'not verified' entries and update
        all_results = [r for r in all_results if r["website"] not in {x["website"] for x in not_verified}]
        all_results.extend(final_sync_results)

    return all_results


# --- STREAMLIT UI LAYOUT ---

st.set_page_config(page_title=" Website URL Validator", page_icon="🔍")

st.title("🔍Website Validator")
st.markdown("""
This tool validates account websites. 
1. **Upload** your CSV (must have `website` column only).
2. **Click** Start.
3. **Download** the results.
""")

input_csv = st.file_uploader("Upload CSV File", type="csv")

if input_csv:
    if st.button("Run Validation"):
        start_time = datetime.now()

        # Run the processing logic
        final_results = process_batches(input_csv)

        if final_results:
            end_time = datetime.now()
            st.success(f"✅ All {len(final_results)} results processed in {end_time - start_time}")

            # Create output dataframe
            output_df = pd.DataFrame(final_results)

            # Step 3: Download Button
            csv_data = output_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Step 3: Download Results CSV",
                data=csv_data,
                file_name=f"Final_Results_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv",
                mime="text/csv"
            )
        else:
            st.error("No valid results were generated.")
