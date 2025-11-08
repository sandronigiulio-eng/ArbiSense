import os, warnings
UA = os.getenv("REQUESTS_UA",
               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/126.0.0.0 Safari/537.36")

# Silenzia i warning di urllib3 (LibreSSL/NotOpenSSL ecc.)
try:
    import urllib3
    from urllib3.exceptions import NotOpenSSLWarning, InsecureRequestWarning
    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
    warnings.filterwarnings("ignore", category=InsecureRequestWarning)
    urllib3.disable_warnings(NotOpenSSLWarning)
    urllib3.disable_warnings(InsecureRequestWarning)
except Exception:
    pass

# Patching globale di requests.Session per forzare l'UA
try:
    import requests
    _orig_init = requests.sessions.Session.__init__
    def _patched_init(self, *a, **kw):
        _orig_init(self, *a, **kw)
        try:
            self.headers.update({"User-Agent": UA})
        except Exception:
            pass
    requests.sessions.Session.__init__ = _patched_init
except Exception:
    pass
