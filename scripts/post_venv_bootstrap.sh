python - <<'PY'
import site, os, pathlib
sp = [p for p in site.getsitepackages() if p.endswith("site-packages")][0]
pth = pathlib.Path(sp)/"zz_arbi_bootstrap.pth"
root = os.getcwd()
pth.write_text(root + "\nimport arbi_bootstrap_site\n")
print("Bootstrapped:", pth)
PY
