from flask import Flask, render_template, request, send_file, jsonify
import tempfile
import os

# ?? IMPORT YOUR ENGINES
from engines.stock_engine import run_stock
from engines.sold_engine import run_sold
from engines.combine_engine import run_combine
from engines.graph_engine import run_graph
from engines.sort_engine import run_sort

app = Flask(__name__)


# =========================================================
# ?? PAGES
# =========================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/stock")
def stock_page():
    return render_template("stock.html")


@app.route("/sold")
def sold_page():
    return render_template("sold.html")


@app.route("/combine")
def combine_page():
    return render_template("combine.html")


@app.route("/graph")
def graph_page():
    return render_template("graph.html")


@app.route("/location")
def location_page():
    return "<h2>Location Wise Page (Coming Soon)</h2>"


# =========================================================
# ?? COMMON FILE HANDLER (AUTO DELETE SAFE)
# =========================================================
def handle_processing(files, process_function, download_name):
    """
    Universal handler:
    - Creates temp directory
    - Passes it to engine
    - Auto deletes after response
    """

    with tempfile.TemporaryDirectory() as temp_dir:

        # ?? Pass temp_dir to your engines (IMPORTANT)
        output = process_function(files, temp_dir)

        return send_file(
            output,
            download_name=download_name,
            as_attachment=True
        )


# =========================================================
# ?? STOCK API
# =========================================================
@app.route("/stock-upload", methods=["POST"])
def stock_upload():
    files = request.files.getlist("files")
    return handle_processing(files, run_stock, "stock_output.zip")


# =========================================================
# ?? SOLD API
# =========================================================
@app.route("/sold-upload", methods=["POST"])
def sold_upload():
    files = request.files.getlist("files")
    return handle_processing(files, run_sold, "sold_output.zip")


# =========================================================
# ?? COMBINE API
# =========================================================
@app.route("/combine-upload", methods=["POST"])
def combine_upload():
    files = request.files.getlist("files")
    return handle_processing(files, run_combine, "combine_output.zip")


# =========================================================
# ?? SORT API
# =========================================================
@app.route("/sort-upload", methods=["POST"])
def sort_upload():
    files = request.files.getlist("files")
    return handle_processing(files, run_sort, "sorted_output.zip")


# =========================================================
# ?? GRAPH API
# =========================================================
@app.route("/graph-data", methods=["GET"])
def graph_data():
    data = run_graph()
    return jsonify(data)


# =========================================================
# RUN APP
# =========================================================
if __name__ == "__main__":
    app.run(debug=True)