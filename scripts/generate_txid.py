import json
import os
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urljoin

import requests

ROOT = Path(__file__).resolve().parent.parent
NODE_DIR = ROOT / "txid-node"
CACHE_DIR = NODE_DIR / "cache"
GEN_DIR = CACHE_DIR / "gen"
META_FILE = CACHE_DIR / "meta.json"
HOME_CACHE = CACHE_DIR / "home.html"
HOME_TTL = 600  # seconds
STATIC = "https://abs.twimg.com/x-web/x-web/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
JS_REF = re.compile(r"[\w@./:-]+\.js\b")
GEN_IMPORT = re.compile(r"import\(`\./([\w.-]+\.js)`\)\.then\(\w+=>\w+\.default\(\)\)")
DEP_IMPORT = re.compile(r'(?:from|import)\s*"\./([\w.-]+\.js)"')


def fetch(session, url):
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return r.text


def get_home(session):
    """Homepage HTML, cached for HOME_TTL seconds; retries on failure."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if HOME_CACHE.exists() and time.time() - HOME_CACHE.stat().st_mtime < HOME_TTL:
        return HOME_CACHE.read_text(encoding="utf-8")

    last = None
    for attempt in range(3):
        try:
            html = fetch(session, "https://x.com/home")
            HOME_CACHE.write_text(html, encoding="utf-8")
            return html
        except Exception as e:
            last = e
            time.sleep(1.5 * (attempt + 1))

    if HOME_CACHE.exists():  # stale copy is better than failing
        return HOME_CACHE.read_text(encoding="utf-8")
    raise last


def discover_generator(session, html):
    """Return (entry_name, generator_url) for the current X build."""
    m = re.search(r"abs\.twimg\.com/x-web/x-web/(entry-client[\w.-]*\.js)", html)
    if not m:
        raise RuntimeError("entry-client script not found in homepage")
    entry_name = m.group(1)
    entry_url = STATIC + entry_name

    entry_js = fetch(session, entry_url)
    candidates = sorted(
        {urljoin(entry_url, r) for r in JS_REF.findall(entry_js)
         if not r.startswith("http") or "twimg.com" in r}
    )
    candidates = [u for u in candidates if "twimg.com" in u]

    def check(url):
        try:
            text = fetch(requests.Session(), url)
        except Exception:
            return None
        if "x-botox-client-transaction-id" in text:
            g = GEN_IMPORT.search(text)
            if g:
                return urljoin(url, "./" + g.group(1))
        return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        for result in pool.map(check, candidates):
            if result:
                return entry_name, result
    raise RuntimeError("transaction-id generator not found in this X build")


def download_module(session, url, seen=None):
    """Download a generator module and its relative imports into GEN_DIR."""
    seen = seen if seen is not None else set()
    name = url.rsplit("/", 1)[-1]
    if name in seen:
        return
    seen.add(name)
    text = fetch(session, url)
    (GEN_DIR / name).write_text(text, encoding="utf-8")
    for dep in DEP_IMPORT.findall(text):
        download_module(session, urljoin(url, "./" + dep), seen)


def ensure_generator(session, html):
    entry = re.search(r"abs\.twimg\.com/x-web/x-web/(entry-client[\w.-]*\.js)", html)
    entry_name = entry.group(1) if entry else None

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    GEN_DIR.mkdir(parents=True, exist_ok=True)

    if META_FILE.exists() and entry_name:
        try:
            meta = json.loads(META_FILE.read_text())
            gen_file = GEN_DIR / meta["generator"]
            if meta.get("entry") == entry_name and gen_file.exists():
                return gen_file
        except Exception:
            pass

    entry_name, gen_url = discover_generator(session, html)
    for old in GEN_DIR.glob("*.js"):
        old.unlink()
    download_module(session, gen_url)
    gen_name = gen_url.rsplit("/", 1)[-1]
    META_FILE.write_text(json.dumps({"entry": entry_name, "generator": gen_name}))
    return GEN_DIR / gen_name


def main():
    if len(sys.argv) != 3:
        print("usage: generate_txid.py <method> <path>", file=sys.stderr)
        sys.exit(1)

    method = sys.argv[1].upper()
    path = (sys.argv[2].split("?")[0] or "").strip()

    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        html = get_home(session)
        gen_file = ensure_generator(session, html)

        with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as f:
            f.write(html)
            html_path = f.name
        try:
            node = os.environ.get("XPLORE_NODE", "node")
            out = subprocess.run(
                [node, str(NODE_DIR / "run.mjs"), method, path, html_path, str(gen_file)],
                capture_output=True, text=True, timeout=30, cwd=str(NODE_DIR),
            )
        finally:
            os.unlink(html_path)

        if out.returncode != 0:
            raise RuntimeError(out.stderr.strip() or "node failed")
        txid = out.stdout.strip().splitlines()[-1]
        if not txid:
            raise RuntimeError("empty transaction id")
        print(txid)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()