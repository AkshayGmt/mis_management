import pandas as pd
import zipfile
from io import BytesIO
import os
import re


# =========================================================
# CONFIG
# =========================================================
KIND_LIST = ["DZ", "FANCY"]
LOCATION_LIST = ["USA", "INDIA"]


# =========================================================
# HEADER CLEANING
# =========================================================
def clean_columns(df):
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


# =========================================================
# SIZE NORMALIZATION
# =========================================================
def normalize_size(x):
    if pd.isna(x):
        return x

    x = str(x).upper().replace("–", "-").replace("—", "-")
    x = re.sub(r"\s+", "", x)

    match = re.findall(r"\d+\.?\d*", x)
    if len(match) >= 2:
        return f"{match[0]}-{match[1]}"
    elif len(match) == 1:
        return match[0]
    return x


# =========================================================
# SAFE COLUMN FINDER
# =========================================================
def find_col(df, keywords):
    for col in df.columns:
        for k in keywords:
            if k in col:
                return col
    return None


# =========================================================
# GROUP FUNCTION
# =========================================================
def group_data(df):

    df = clean_columns(df)

    shape = find_col(df, ["shape"])
    size = find_col(df, ["size"])
    color = find_col(df, ["color"])
    clarity = find_col(df, ["clarity"])
    lab = find_col(df, ["lab"])
    type_ = find_col(df, ["type"])

    carat = find_col(df, ["carat", "cts", "weight"])
    amount = find_col(df, ["amount", "price", "value"])
    count = find_col(df, ["pcs", "count", "qty"])

    group_cols = [c for c in [shape, size, color, clarity, lab, type_] if c]

    agg_dict = {}
    if carat:
        agg_dict[carat] = "sum"
    if amount:
        agg_dict[amount] = "sum"
    if count:
        agg_dict[count] = "sum"

    if not group_cols or not agg_dict:
        return pd.DataFrame()

    df = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()

    # rename
    rename = {}
    if carat:
        rename[carat] = "Carat"
    if amount:
        rename[amount] = "Amount"
    if count:
        rename[count] = "Count"

    df.rename(columns=rename, inplace=True)

    return df


# =========================================================
# CLASSIFY FILES
# =========================================================
def classify(df):

    df = clean_columns(df)

    color_col = find_col(df, ["color"])
    loc_col = find_col(df, ["location", "country", "origin"])

    if color_col:
        df[color_col] = df[color_col].astype(str).str.upper()

        fancy = df[df[color_col].str.startswith("FANCY")]
        dz = df[~df.index.isin(fancy.index)]
    else:
        fancy = df
        dz = df

    def split_location(data):

        if loc_col is None:
            return data, pd.DataFrame()

        data[loc_col] = data[loc_col].astype(str).str.upper()

        usa = data[data[loc_col].str.contains("USA|US|AMERICA", na=False)]
        india = data[~data[loc_col].str.contains("USA|US|AMERICA", na=False)]

        return usa, india

    return {
        "FANCY": split_location(fancy),
        "DZ": split_location(dz)
    }


# =========================================================
# MAIN ENGINE
# =========================================================
def run_combine(files):

    memory = BytesIO()

    with zipfile.ZipFile(memory, "w") as z:

        for f in files:

            try:
                df = pd.read_excel(f)
            except:
                continue

            data = classify(df)

            base = os.path.splitext(f.filename)[0]

            merged_final = {}

            for kind in KIND_LIST:
                merged_final[kind] = {}

                for loc in LOCATION_LIST:

                    df_list = data[kind][0] if loc == "USA" else data[kind][1]

                    if df_list.empty:
                        continue

                    grouped = group_data(df_list)

                    if grouped.empty:
                        continue

                    grouped["Category"] = kind

                    # TOTAL ROW
                    total = grouped.copy()
                    for col in grouped.columns:
                        if grouped[col].dtype != "object":
                            total[col] = grouped[col].sum()
                        else:
                            total[col] = "TOTAL"

                    grouped = pd.concat([total.head(1), grouped], ignore_index=True)

                    buf = BytesIO()
                    grouped.to_excel(buf, index=False)
                    buf.seek(0)

                    filename = f"{base}_{kind}_{loc}.xlsx"
                    z.writestr(filename, buf.getvalue())

    memory.seek(0)
    return memory
