import os

from dotenv import load_dotenv

from app import create_app

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = create_app(BASE_DIR)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5201, debug=False, threaded=True)