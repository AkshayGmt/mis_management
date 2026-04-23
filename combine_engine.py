import pandas as pd
import zipfile
import os
from io import BytesIO


# =========================================================
# 🔥 GLOBAL STORAGE (replaces Flask memory)
# =========================================================

files_data = []


# =========================================================
# 🔥 HEADER DETECTION (UNCHANGED LOGIC)
# =========================================================

def detect_header(df):
    keywords = ["shape", "color", "clarity", "location", "country", "origin"]

    for i, row in df.iterrows():
        vals = [str(x).lower() for x in row.values]
        if sum(any(k in v for v in vals) for k in keywords) >= 2:
            return i
    return None


# =========================================================
# 🔥 FILE PROCESSOR (UNCHANGED)
# =========================================================

def process_file(path):
    try:
        df = pd.read_excel(path, header=None)
        h = detect_header(df)
        if h is None:
            return None
        df = pd.read_excel(path, header=h)
        return df
    except:
        return None


# =========================================================
# 🔥 DATA BUILDER (UNCHANGED LOGIC)
# =========================================================

def build_data():

    categories = list(set(f["type"] for f in files_data))

    kinds = ["DZ", "FANCY"]
    locations = ["USA", "INDIA"]

    result = {
        c: {k: {"USA": [], "INDIA": []} for k in kinds}
        for c in categories
    }

    for f in files_data:

        df = process_file(f["path"])
        if df is None:
            continue

        cat = f["type"]
        kind = f["kind"]

        cols = [c.lower().strip() for c in df.columns]

        loc_col = None
        for x in ["location", "country", "origin"]:
            if x in cols:
                loc_col = df.columns[cols.index(x)]
                break

        if loc_col is None:
            result[cat][kind]["INDIA"].append(df)
            continue

        df[loc_col] = df[loc_col].astype(str).str.lower()

        usa = df[df[loc_col].str.contains("usa|us|america", na=False)]
        india = df[~df[loc_col].str.contains("usa|us|america", na=False)]

        if not usa.empty:
            result[cat][kind]["USA"].append(usa)
        if not india.empty:
            result[cat][kind]["INDIA"].append(india)

    return result, categories


# =========================================================
# 🔥 COLUMN FINDER (UNCHANGED)
# =========================================================

def find(df, names):
    for n in names:
        for c in df.columns:
            if n in c:
                return c
    return None


# =========================================================
# 🔥 GROUP FUNCTION (UNCHANGED)
# =========================================================

def group_df(df):

    df.columns = [c.lower().strip() for c in df.columns]

    gcols = []

    for key in ["shape", "size", "color", "clarity", "lab", "type"]:
        for c in df.columns:
            if key in c:
                gcols.append(c)
                break

    count = find(df, ["pcs", "qty", "count"])
    carat = find(df, ["carat", "cts", "weight"])
    amount = find(df, ["amount", "value", "price"])

    agg = {}
    if count: agg[count] = "sum"
    if carat: agg[carat] = "sum"
    if amount: agg[amount] = "sum"

    if not gcols or not agg:
        return pd.DataFrame()

    df = df.groupby(gcols, dropna=False).agg(agg).reset_index()

    rename = {}
    if count: rename[count] = "count"
    if carat: rename[carat] = "carat"
    if amount: rename[amount] = "amount"

    return df.rename(columns=rename)


# =========================================================
# 🚀 COMBINE ENGINE MAIN FUNCTION
# =========================================================

def run_combine(files):

    # store uploaded files into engine memory
    for f in files:
        path = f"uploads/{f.filename}"
        os.makedirs("uploads", exist_ok=True)
        f.save(path)

        files_data.append({
            "name": f.filename,
            "type": "UNKNOWN",
            "kind": "DZ" if "dz" in f.filename.lower() else "FANCY",
            "path": path
        })

    kinds = ["DZ", "FANCY"]
    locations = ["USA", "INDIA"]

    data, categories = build_data()

    mem = BytesIO()

    with zipfile.ZipFile(mem, "w") as z:

        for k in kinds:
            for l in locations:

                merged = None

                for c in categories:

                    dfs = data[c][k][l]
                    if not dfs:
                        continue

                    df = pd.concat(dfs, ignore_index=True)
                    g = group_df(df)

                    if g.empty:
                        continue

                    g = g.rename(columns={
                        "count": f"{c.lower()} count",
                        "carat": f"{c.lower()} carat",
                        "amount": f"{c.lower()} amount"
                    })

                    merged = g if merged is None else pd.merge(merged, g, how="outer")

                if merged is None:
                    continue

                merged.fillna(0, inplace=True)

                total = {
                    c: merged[c].sum() if merged[c].dtype != "object" else "TOTAL"
                    for c in merged.columns
                }

                merged = pd.concat([pd.DataFrame([total]), merged], ignore_index=True)

                buf = BytesIO()
                merged.to_excel(buf, index=False)

                z.writestr(f"{k}_{l}.xlsx", buf.getvalue())

    mem.seek(0)

    # cleanup
    for f in files_data:
        try:
            os.remove(f["path"])
        except:
            pass

    files_data.clear()

    return mem
