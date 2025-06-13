from dotenv import load_dotenv
import os


load_dotenv(verbose=True)

"""
    Extract environment variables
"""

TEST_CONFIG = {
    k: os.environ.get(k, None)
    for k in [
        "ENV",
        "SUBSCRIPTION_ID",
        "PURVIEW_ID"
    ]
}
