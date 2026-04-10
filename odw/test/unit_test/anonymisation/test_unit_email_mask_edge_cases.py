import pytest
from odw.core.anonymisation.base import EmailMaskStrategy
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import functions as F


def test__email_mask_strategy__handles_edge_cases():
    """
    Test that EmailMaskStrategy correctly distinguishes between valid emails
    and strings that just happen to contain '@'.
    
    This test documents current behavior - the strategy is overly permissive
    and treats any string with '@' as an email, which may not be desired.
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    # Test cases with expected behavior
    test_data = [
        # (input, description, is_valid_email)
        ("john.doe@example.com", "Standard valid email", True),
        ("a@x.com", "Minimal valid email (single char local)", True),
        ("abc@", "Invalid: no domain", False),
        ("ab@x.com", "Valid: 2 char local part", True),
        ("@domain.com", "Invalid: no local part", False),
        ("not an email @ value", "Invalid: @ in middle of text", False),
        ("user@domain", "Invalid: no TLD", False),
        ("@", "Invalid: just @ symbol", False),
        ("@@@@", "Invalid: multiple @ symbols", False),
        ("Price @ £100", "Invalid: @ used as 'at' symbol", False),
        ("Meeting @ 3pm", "Invalid: @ in sentence", False),
        ("user@@example.com", "Invalid: double @@", False),
        ("user@example..com", "Invalid: double dot", False),
        ("", "Empty string", False),
        (None, "Null value", False),
        ("valid.email+tag@sub.example.com", "Valid: complex email", True),
        ("user@localhost", "Edge case: no TLD (debatable)", False),
    ]

    data = [(value, desc) for value, desc, _ in test_data]
    df = spark.createDataFrame(data, ["email", "description"])

    # Apply the email masking strategy
    strategy = EmailMaskStrategy()
    seed = F.lit("test_seed")
    result_df = strategy.apply(df, "email", seed, {})

    # Collect results
    results = result_df.select("email", "description").collect()

    # Print results for inspection
    print("\nEmail Masking Results:")
    print("-" * 80)
    for i, row in enumerate(results):
        original = test_data[i][0]
        masked = row["email"]
        description = row["description"]
        is_valid = test_data[i][2]
        
        # Check if it was treated as an email (has @pins.com or @ in result)
        treated_as_email = masked is not None and "@" in str(masked) if masked else False
        
        status = "✓" if treated_as_email == is_valid else "✗ MISMATCH"
        print(f"{status} | {description:40s} | '{original}' -> '{masked}'")

    # Document specific test expectations
    
    # Valid emails should be masked as email format
    valid_email_row = [r for r in results if r["description"] == "Standard valid email"][0]
    assert valid_email_row["email"] == "j******e@pins.com", \
        "Valid email should be masked with @pins.com domain"
    
    # Minimal valid email
    minimal_email_row = [r for r in results if r["description"] == "Minimal valid email (single char local)"][0]
    assert minimal_email_row["email"] == "a@pins.com", \
        "Single character local part should be preserved with @pins.com"
    
    # FIXED: Invalid cases are now correctly treated as non-email
    # Note: Non-email masking keeps special characters like @ unchanged
    
    no_domain_row = [r for r in results if r["description"] == "Invalid: no domain"][0]
    # FIXED: "abc@" is now treated as non-email -> "a**@" (@ is preserved)
    assert no_domain_row["email"] == "a**@", \
        "'abc@' should be treated as non-email (@ preserved in mask)"
    assert "@pins.com" not in no_domain_row["email"], \
        "'abc@' should NOT be treated as a valid email"
    
    no_local_row = [r for r in results if r["description"] == "Invalid: no local part"][0]
    # FIXED: "@domain.com" is now treated as non-email -> "@*********m"
    assert no_local_row["email"] == "@*********m", \
        "'@domain.com' should be treated as non-email"
    assert "@pins.com" not in no_local_row["email"], \
        "'@domain.com' should NOT be treated as a valid email"
    
    text_with_at_row = [r for r in results if r["description"] == "Invalid: @ in middle of text"][0]
    # FIXED: "not an email @ value" is now treated as non-email
    assert text_with_at_row["email"] == "n******************e", \
        "Text with @ should be treated as non-email"
    assert "@pins.com" not in text_with_at_row["email"], \
        "Text with @ should NOT be treated as a valid email"
    
    # Nulls should remain null
    null_row = [r for r in results if r["description"] == "Null value"][0]
    assert null_row["email"] is None, "Null values should remain null"
    
    # Double dots should not be treated as valid email
    double_dot_row = [r for r in results if r["description"] == "Invalid: double dot"][0]
    assert "@pins.com" not in double_dot_row["email"], \
        "'user@example..com' should NOT be treated as a valid email (consecutive dots)"
    assert double_dot_row["email"] == "u***************m", \
        "'user@example..com' should be masked as non-email"


@pytest.mark.skip(reason="Demonstrates improved implementation - not yet merged")
def test__email_mask_strategy__with_improved_validation():
    """
    This test shows how the EmailMaskStrategy SHOULD behave with proper
    email validation using a regex pattern.
    
    Skip this test until the improved implementation is merged.
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    test_data = [
        ("john.doe@example.com", "j******e@pins.com"),  # Valid
        ("a@x.com", "a@pins.com"),  # Valid minimal
        ("abc@", "a*c"),  # Invalid: treat as non-email
        ("@domain.com", "@*******.**m"),  # Invalid: treat as non-email
        ("not an email @ value", "n************e"),  # Invalid: treat as non-email
        ("Price @ £100", "P****e"),  # Invalid: treat as non-email
        (None, None),  # Null remains null
    ]

    data = [(value,) for value, _ in test_data]
    df = spark.createDataFrame(data, ["email"])

    # TODO: This would require updating EmailMaskStrategy with proper regex
    strategy = EmailMaskStrategy()
    seed = F.lit("test_seed")
    result_df = strategy.apply(df, "email", seed, {})

    results = result_df.select("email").collect()
    
    for i, row in enumerate(results):
        expected = test_data[i][1]
        actual = row["email"]
        assert actual == expected, f"Expected '{expected}', got '{actual}'"


def test__email_mask_strategy__does_not_crash_on_edge_cases():
    """
    Ensure the strategy doesn't crash on unusual inputs, even if masking
    behavior is not ideal.
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    # Edge cases that might cause crashes
    edge_cases = [
        "",           # Empty string
        " ",          # Space
        "@",          # Just @
        "@@",         # Multiple @
        "@" * 100,    # Many @
        "a" * 1000,   # Very long string
        "a@" + "b" * 1000,  # Very long domain
        "\n@\n",      # @ with newlines
        "test\t@\tvalue",  # @ with tabs
    ]

    data = [(value,) for value in edge_cases]
    df = spark.createDataFrame(data, ["email"])

    strategy = EmailMaskStrategy()
    seed = F.lit("test_seed")
    
    # Should not crash
    result_df = strategy.apply(df, "email", seed, {})
    results = result_df.collect()
    
    assert len(results) == len(edge_cases), "All rows should be processed"
    print("\nEdge case handling (no crashes):")
    for i, row in enumerate(results):
        print(f"  '{edge_cases[i]}' -> '{row['email']}'")
