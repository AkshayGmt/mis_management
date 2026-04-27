from flask import Flask, request, jsonify, render_template, send_file
import pandas as pd
import os, zipfile
from io import BytesIO

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

files_data = []


@app.route("/")
def home():
    return render_template("index.html")


# -------------------- HEADER DETECTION --------------------
def detect_header(df):
    keywords = ["shape","color","clarity","location","country","origin"]
    for i, row in df.iterrows():
        vals = [str(x).lower() for x in row.values]
        if sum(any(k in v for v in vals) for k in keywords) >= 2:
            return i
    return None


def process_file(path):
    try:
        df = pd.read_excel(path, header=None)
        h = detect_header(df)
        if h is None:
            print("Header not found:", path)
            return None
        return pd.read_excel(path, header=h)
    except Exception as e:
        print("Error reading:", path, e)
        return None


def find(df, names):
    for n in names:
        for c in df.columns:
            if n in c:
                return c
    return None


# -------------------- SIZE NORMALIZATION --------------------
def normalize_size(val):
    if pd.isna(val):
        return val
    val = str(val).strip()

    # remove spaces around hyphen
    val = val.replace(" - ", "-")
    val = val.replace("- ", "-")
    val = val.replace(" -", "-")

    # ensure single clean format
    parts = val.split("-")
    if len(parts) == 2:
        try:
            a = str(float(parts[0].strip()))
            b = str(float(parts[1].strip()))
            return f"{a}-{b}"
        except:
            return val
    return val


# -------------------- GROUPING --------------------
def group_df(df):
    df.columns = [c.lower().strip() for c in df.columns]

    gcols = []
    size_col = None

    for key in ["shape","size","color","clarity","lab","type"]:
        for c in df.columns:
            if key in c:
                gcols.append(c)
                if key == "size":
                    size_col = c
                break

    # normalize size column
    if size_col:
        df[size_col] = df[size_col].apply(normalize_size)

    count = find(df, ["pcs","qty","count"])
    carat = find(df, ["carat","cts","weight"])
    amount = find(df, ["amount","value","price"])

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

    # -------------------- AVG COLUMN --------------------
    if "carat" in df.columns and "amount" in df.columns:
        df["avg"] = df.apply(
            lambda x: (x["amount"] / x["carat"]) if x["carat"] else 0,
            axis=1
        )

    return df


# -------------------- DATA BUILD --------------------
def build_data():
    categories = list(set(f["type"] for f in files_data))
    kinds = ["DZ","FANCY"]

    result = {c:{k:{"USA":[],"INDIA":[]} for k in kinds} for c in categories}

    for f in files_data:

        df = process_file(f["path"])
        if df is None:
            continue

        cat, kind = f["type"], f["kind"]

        cols = [c.lower().strip() for c in df.columns]
        loc_col = None

        for x in ["location","country","origin"]:
            if x in cols:
                loc_col = df.columns[cols.index(x)]
                break

        if loc_col is None:
            result[cat][kind]["INDIA"].append(df)
            continue

        df[loc_col] = df[loc_col].astype(str).str.lower()

        usa = df[df[loc_col].str.contains("usa|us|america", na=False)]
        india = df[~df[loc_col].str.contains("usa|us|america", na=False)]

        if not usa.empty: result[cat][kind]["USA"].append(usa)
        if not india.empty: result[cat][kind]["INDIA"].append(india)

    return result, categories


# -------------------- PREVIEW --------------------
@app.route("/process-preview")
def preview():

    data, categories = build_data()
    kinds = ["DZ","FANCY"]
    locations = ["USA","INDIA"]

    output = {}

    for k in kinds:
        output[k] = {}

        for l in locations:

            merged = None

            for c in categories:

                dfs = data[c][k][l]
                if not dfs: continue

                df = pd.concat(dfs, ignore_index=True)
                g = group_df(df)

                if g.empty: continue

                g = g.rename(columns={
                    "count": f"{c.lower()} count",
                    "carat": f"{c.lower()} carat",
                    "amount": f"{c.lower()} amount",
                    "avg": f"{c.lower()} avg"
                })

                merged = g if merged is None else pd.merge(merged, g, how="outer")

            if merged is None:
                output[k][l] = []
                continue

            merged.fillna(0, inplace=True)
            output[k][l] = merged.head(10).to_dict("records")

    return jsonify({"data": output})


# -------------------- DOWNLOAD --------------------
@app.route("/download")
def download():

    data, categories = build_data()
    kinds = ["DZ","FANCY"]
    locations = ["USA","INDIA"]

    mem = BytesIO()

    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:

        file_added = False

        for k in kinds:
            for l in locations:

                merged = None

                for c in categories:

                    dfs = data[c][k][l]
                    if not dfs: continue

                    df = pd.concat(dfs, ignore_index=True)
                    g = group_df(df)

                    if g.empty: continue

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
                    col: (merged[col].sum() if merged[col].dtype != "object" else "TOTAL")
                    for col in merged.columns
                }

                merged = pd.concat([pd.DataFrame([total]), merged], ignore_index=True)

                buf = BytesIO()
                merged.to_excel(buf, index=False)

                z.writestr(f"{k}_{l}.xlsx", buf.getvalue())
                file_added = True

        if not file_added:
            z.writestr("NO_DATA.txt","No valid data found")

    mem.seek(0)

    response = send_file(
        mem,
        download_name="MIS_Output.zip",
        as_attachment=True,
        mimetype="application/zip"
    )

    for f in files_data:
        try: os.remove(f["path"])
        except: pass
    files_data.clear()

    return response


# -------------------- UPLOAD --------------------
@app.route("/upload", methods=["POST"])
def upload():

    for f in request.files.getlist("files"):

        path = os.path.join(UPLOAD_FOLDER, f.filename)
        f.save(path)

        files_data.append({
            "name": f.filename,
            "type": request.form.get("type"),
            "kind": "DZ" if "dz" in f.filename.lower() else "FANCY",
            "path": path
        })

    return "OK"


@app.route("/files")
def files():
    return jsonify(files_data)


@app.route("/move", methods=["POST"])
def move():
    d = request.json
    for f in files_data:
        if f["name"] == d["name"]:
            f["type"] = d["type"]
            f["kind"] = d["kind"]
    return "OK"


@app.route("/delete", methods=["POST"])
def delete():
    global files_data
    name = request.json["name"]

    for f in files_data:
        if f["name"] == name:
            try: os.remove(f["path"])
            except: pass

    files_data = [f for f in files_data if f["name"] != name]
    return "OK"


if __name__ == "__main__":
    app.run(debug=True)
