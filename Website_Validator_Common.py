import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import nest_asyncio
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from yarl import URL
from datetime import datetime

# Required for async logic within Streamlit
nest_asyncio.apply()

# --- CONFIGURATION ---
CONCURRENCY = 100 
TIMEOUT = 30

# --- UTILITY FUNCTIONS ---

def ensure_scheme(url):
    try:
        if not url or pd.isna(url): return None
        url = str(url).strip()
        parsed = URL(url)
        if not parsed.scheme:
            return f"https://{url}"
        return url
    except Exception:
        return url

def normalize_url(url):
    try:
        parsed = URL(url)
        if parsed.host is None: return None
        hostname = parsed.host.lower()
        if hostname.startswith("www."):
            hostname = hostname[4:]
        return hostname.split('.')[0]
    except:
        return None

async def check_website(session, url, retries=2):
    original_url = url
    target_url = ensure_scheme(url)
    
    if not target_url:
        return {"website": original_url, "status": "Invalid URL", "redirect_url": None, "timestamp": datetime.now()}

    for attempt in range(retries + 1):
        try:
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
            async with session.get(target_url, timeout=TIMEOUT, allow_redirects=False, headers=headers) as response:
                code = response.status
                
                if 200 <= code < 300:
                    return {"website": original_url, "status": f"Working {code}", "redirect_url": None, "timestamp": datetime.now()}
                
                elif 300 <= code < 400:
                    location = response.headers.get("Location", "")
                    redirect_url = str(URL(target_url).join(URL(location)))
                    
                    # If it's just a protocol change (http to https), mark as working
                    if normalize_url(target_url) == normalize_url(redirect_url):
                        return {"website": original_url, "status": f"Working {code}", "redirect_url": None, "timestamp": datetime.now()}
                    
                    return {"website": original_url, "status": f"Redirected {code}", "redirect_url": redirect_url, "timestamp": datetime.now()}
                
                else:
                    status = f"Non Working {code}" if code not in {401, 403, 429} else f"Working but Restricted {code}"
                    return {"website": original_url, "status": status, "redirect_url": None, "timestamp": datetime.now()}
                    
        except Exception as e:
            if attempt < retries:
                await asyncio.sleep(1)
                continue
            return {"website": original_url, "status": f"Not Able to Verify - {type(e).__name__}", "redirect_url": None, "timestamp": datetime.now()}
            
    return {"website": original_url, "status": "Not Able to Verify", "redirect_url": None, "timestamp": datetime.now()}

def sync_fallback(url):
    try:
        target = ensure_scheme(url)
        response = requests.get(target, timeout=20, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        return {"website": url, "status": f"Working {response.status_code} (Sync)", "redirect_url": None, "timestamp": datetime.now()}
    except Exception:
        return {"website": url, "status": "Non Working (Final)", "redirect_url": None, "timestamp": datetime.now()}

# --- PROCESSING ENGINE ---

async def run_bulk_validation(urls, progress_bar, status_text):
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    results = []
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [check_website(session, url) for url in urls]
        for i, completed_task in enumerate(asyncio.as_completed(tasks)):
            res = await completed_task
            results.append(res)
            if i % 5 == 0:
                progress_bar.progress((i + 1) / len(urls))
                status_text.text(f"Validated {i+1} of {len(urls)} websites...")
    return results

# --- STREAMLIT UI ---

st.set_page_config(page_title="Website Validator", page_icon="🌐")

st.title("🌐 Website Status Validator")
st.markdown("Upload a CSV containing a column named **'website'** to check status.")

uploaded_file = st.file_uploader("Upload CSV", type="csv")

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    
    if 'website' not in df.columns:
        st.error("Error: CSV must contain a column named 'website'.")
    else:
        urls = df['website'].dropna().unique().tolist()
        st.info(f"Found {len(urls)} unique websites to validate.")
        
        if st.button("🚀 Start Validation"):
            status_text = st.empty()
            progress_bar = st.progress(0)
            
            # Phase 1: Async
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            with st.spinner("Batch processing URLs..."):
                results = loop.run_until_complete(run_bulk_validation(urls, progress_bar, status_text))
                
                # Phase 2: Sync Fallback for failures
                unverified = [r for r in results if "Not Able to Verify" in r["status"]]
                if unverified:
                    status_text.text(f"Retrying {len(unverified)} difficult URLs with secondary method...")
                    with ThreadPoolExecutor(max_workers=15) as executor:
                        futures = [executor.submit(sync_fallback, r['website']) for r in unverified]
                        sync_results = [f.result() for f in as_completed(futures)]
                    
                    # Update results list
                    results = [r for r in results if r['website'] not in [x['website'] for x in unverified]]
                    results.extend(sync_results)

            status_text.success("✅ Validation Finished!")
            
            # Show and Download Results
            res_df = pd.DataFrame(results)
            st.dataframe(res_df)
            
            csv_out = res_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Report",
                data=csv_out,
                file_name=f"website_validation_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
