import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import nest_asyncio
import requests
import re
import os
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import SSLError, RequestException
from yarl import URL
from datetime import datetime

# Required to run async loops inside the Streamlit web server
nest_asyncio.apply()

# --- CONFIGURATION ---
BATCH_SIZE = 2500
MAX_RECORDS = 100000
CONCURRENCY = 150  # Lowered slightly for web stability
TIMEOUT = 40


# --- UTILITY FUNCTIONS ---

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
    parsed = URL(url)
    if parsed.host is None: return None
    hostname = parsed.host.lower()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname.split('.')[0]


async def check(session, url, account_id, retries=3):
    original_url = url
    url = ensure_scheme(url)
    for attempt in range(retries + 1):
        try:
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
            async with session.get(url, timeout=TIMEOUT, allow_redirects=False, headers=headers) as response:
                code = response.status
                if 200 <= code < 300:
                    return {"sfid": account_id, "website": original_url, "status": f"Working {code}",
                            "redirect_url": None, "createddate": datetime.now()}
                elif 300 <= code < 400:
                    redirect_url = str(URL(url).join(URL(response.headers.get("Location", ""))))
                    if normalize_url(url) == normalize_url(redirect_url):
                        return {"sfid": account_id, "website": original_url, "status": f"Working {code}",
                                "redirect_url": None, "createddate": datetime.now()}
                    return {"sfid": account_id, "website": original_url, "status": f"Redirected {code}",
                            "redirect_url": redirect_url, "createddate": datetime.now()}
                else:
                    status = f"Non Working {code}" if code not in {401, 403, 429} else f"Working but Restricted {code}"
                    return {"sfid": account_id, "website": original_url, "status": status, "redirect_url": None,
                            "createddate": datetime.now()}
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(1)
                continue
            return {"sfid": account_id, "website": original_url, "status": f"Not Able to Verify - {type(e).__name__}",
                    "redirect_url": None, "createddate": datetime.now()}
    return {"sfid": account_id, "website": original_url, "status": "Not Able to Verify", "redirect_url": None,
            "createddate": datetime.now()}


def sync_check_fallback(url, account_id):
    try:
        response = requests.get(ensure_scheme(url), timeout=25, verify=False)
        return {"sfid": account_id, "website": url, "status": f"Working {response.status_code}", "redirect_url": None,
                "createddate": datetime.now()}
    except Exception:
        return {"sfid": account_id, "website": url, "status": "Non Working (Final)", "redirect_url": None,
                "createddate": datetime.now()}


# --- CORE PROCESSING LOGIC ---

async def run_validation(records, progress_bar, status_text):
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    all_results = []

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [check(session, row.website, row.sfid) for row in records]
        for i, completed_task in enumerate(asyncio.as_completed(tasks)):
            res = await completed_task
            all_results.append(res)
            # Update UI Progress
            if i % 10 == 0:
                pct = (i + 1) / len(records)
                progress_bar.progress(pct)
                status_text.text(f"Processed {i + 1} of {len(records)} URLs...")
    return all_results


# --- STREAMLIT UI ---

st.set_page_config(page_title="Salesforce URL Validator", page_icon="🔍")

st.title("🔍 Salesforce Website Validator")
st.markdown("""
This tool validates account websites. 
1. **Upload** your CSV (must have `website` and `sfid` columns).
2. **Click** Start.
3. **Download** the results.
""")

uploaded_file = st.file_uploader("Upload CSV File", type="csv")

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.write(f"📊 Total Records Found: **{len(df)}**")

    if st.button("🚀 Start Validation Process"):
        # Setup UI components for progress
        status_text = st.empty()
        progress_bar = st.progress(0)

        # Prepare data
        records = list(df.itertuples(index=False))

        # Execute Async Logic
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        with st.spinner("Validating URLs..."):
            results = loop.run_until_complete(run_validation(records, progress_bar, status_text))

            # Fallback for "Not Able to Verify"
            not_verified = [r for r in results if "Not Able to Verify" in r["status"]]
            if not_verified:
                status_text.text(f"Running secondary check on {len(not_verified)} difficult URLs...")
                with ThreadPoolExecutor(max_workers=20) as executor:
                    futures = [executor.submit(sync_check_fallback, r['website'], r['sfid']) for r in not_verified]
                    sync_results = [f.result() for f in as_completed(futures)]

                # Replace unverified with sync results
                results = [r for r in results if r['sfid'] not in {x['sfid'] for x in not_verified}]
                results.extend(sync_results)

        status_text.success("✅ Validation Complete!")

        # Display Preview & Download
        result_df = pd.DataFrame(results)
        st.dataframe(result_df.head(10))

        csv_buffer = result_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Results CSV",
            data=csv_buffer,
            file_name=f"Validation_Results_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )