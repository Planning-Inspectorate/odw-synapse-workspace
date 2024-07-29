import pytest
import json

def test_source_to_processed_workflow():
    print("Running Test")
    assert True is False
    print("Test Completed")

@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    yield
    print("Before and After running")
