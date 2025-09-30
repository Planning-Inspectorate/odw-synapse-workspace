from dotenv import load_dotenv
import os


load_dotenv(verbose=True, override=True)

"""
    Extract environment variables
"""

CONFIG = {
    k: os.environ.get(k, None)
    for k in [
        "ENV",
        "DATA_LAKE_STORAGE",
        "SUBSCRIPTION_ID",
        "PURVIEW_ID"
    ]
}
