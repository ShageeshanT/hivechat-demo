import glob, importlib, traceback, sys
sys.path.insert(0, ".")
for f in glob.glob("tests/test_*.py"):
    mod = f.replace(".py", "").replace("\\", ".").replace("/", ".")
    try:
        importlib.import_module(mod)
        print(f"OK: {mod}")
    except Exception as e:
        print(f"FAILED: {mod}")
        traceback.print_exc()
