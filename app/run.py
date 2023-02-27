"""Complete me
"""

import pandas as pd
from flask import Flask, render_template, request

from src.utils import load_parquet, load_pickle, load_yaml, parse_arguments

app = Flask(__name__)


@app.route('/')
@app.route('/index')
def index():
    """Complete me
    """
    return render_template('report.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3001, debug=True)
