from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import zipfile
import os
from io import BytesIO
import re

app = Flask(__name__)

# =========================================================
# GLOBAL STORAGE
# =========================================================
files_data = []


# =========================================================
# SIZE NORMALIZATION (🔥 FIXED)
# =========================================================
def normalize_size(value):
    if pd.isna(value):
        return value

    v = str(value).lower().strip()

    # remove spaces
    v = re.sub(r"\s+", "", v)

    # unify dash types
    v = v.replace("–", "-").replace("—", "-")

    # ensure proper range format
    match = re.match(r"(\d+\.?\d*)-(\d+\.?\d*)", v)
    if match:
        a, b = match.groups()
        return f"{a}-{b}"

    return v


# =========================================================
# HEADER DETECTION
# =========================================================
def detect_header(df):
    keywords = ["shape", "color", "clarity", "location", "country", "origin"]

    for i, row in df.iterrows():
        vals = [str(x).lower() for x in row.values]
        if sum(any(k in v for v in vals) for k in keywords) >= 2:
            return i
    return None


# =========================================================
# PROCESS FILE
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
# BUILD DATA
# =========================================================
def build_data():

    categories = list(set(f["type"] for f in files_data))
    kinds = ["DZ", "FANCY"]

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

        # apply size normalization
        for c in df.columns:
            if "size" in c.lower():
                df[c] = df[c].apply(normalize_size)

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
# COLUMN FINDER
# =========================================================
def find(df, names):
    for n in names:
        for c in df.columns:
            if n in c:
                return c
    return None


# =========================================================
# GROUP FUNCTION (🔥 UPDATED WITH AVG)
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

    df = df.rename(columns=rename)

    # 🔥 AVG COLUMN
    if "amount" in df.columns and "carat" in df.columns:
        df["avg"] = df["amount"] / df["carat"].replace(0, pd.NA)

    return df


# =========================================================
# COMBINE ENGINE
# =========================================================
def run_combine():

    kinds = ["DZ", "FANCY"]
    locations = ["USA", "INDIA"]

    data, categories = build_data()

    # 🔥 ORDER FIX
    order = ["TOTAL", "SOLD", "CURRENT"]
    categories = sorted(set(categories), key=lambda x: order.index(x) if x in order else 999)

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
                        "amount": f"{c.lower()} amount",
                        "avg": f"{c.lower()} avg"
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

                # optional sort if size exists
                for c in merged.columns:
                    if "size" in c:
                        merged = merged.sort_values(by=c)

                buf = BytesIO()
                merged.to_excel(buf, index=False)

                z.writestr(f"{k}_{l}.xlsx", buf.getvalue())

    mem.seek(0)
    return mem


# =========================================================
# ROUTES
# =========================================================
@app.route("/")
def home():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():

    os.makedirs("uploads", exist_ok=True)

    ftype = request.form.get("type")

    for f in request.files.getlist("files"):
        path = os.path.join("uploads", f.filename)
        f.save(path)

        files_data.append({
            "name": f.filename,
            "type": ftype,
            "kind": "DZ" if "dz" in f.filename.lower() else "FANCY",
            "path": path
        })

    return "OK"


@app.route("/files")
def get_files():
    return jsonify(files_data)


@app.route("/delete", methods=["POST"])
def delete():
    global files_data
    name = request.json["name"]
    files_data = [f for f in files_data if f["name"] != name]
    return "OK"


@app.route("/move", methods=["POST"])
def move():
    data = request.json

    for f in files_data:
        if f["name"] == data["name"]:
            f["type"] = data["type"]
            f["kind"] = data["kind"]

    return "OK"


@app.route("/process-preview")
def preview():

    data, categories = build_data()

    preview = {
        k: {"USA": [], "INDIA": []}
        for k in ["DZ", "FANCY"]
    }

    for k in preview:
        for l in preview[k]:
            for c in categories:
                dfs = data[c][k][l]
                for df in dfs:
                    preview[k][l].append(df.head(5).to_dict(orient="records"))

    return jsonify({"data": preview})


@app.route("/download")
def download():
    zip_file = run_combine()
    return send_file(zip_file, download_name="MIS_Output.zip", as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)
