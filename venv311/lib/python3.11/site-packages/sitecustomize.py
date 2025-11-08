import os, warnings

# --- Silenzia warning urllib3 (LibreSSL/NotOpenSSL, ecc.) ---
try:
    import urllib3
    from urllib3.exceptions import NotOpenSSLWarning, InsecureRequestWarning
    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
    warnings.filterwarnings("ignore", category=InsecureRequestWarning)
    urllib3.disable_warnings(NotOpenSSLWarning)
    urllib3.disable_warnings(InsecureRequestWarning)
except Exception:
    pass

# --- Forza User-Agent per TUTTE le requests.Session ---
UA = os.getenv("REQUESTS_UA",
               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/126.0.0.0 Safari/537.36")

try:
    import requests as _requests
    _orig_init = _requests.sessions.Session.__init__
    def _patched_init(self, *args, **kwargs):
        _orig_init(self, *args, **kwargs)
        try:
            self.headers.update({"User-Agent": UA})
        except Exception:
            pass
    _requests.sessions.Session.__init__ = _patched_init
except Exception:
    pass
