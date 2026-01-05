import json

def lambda_handler(event, context):
    """
    Reconciliation Lambda
    Compares RAW vs CURATED record counts.
    Raises exception on mismatch.
    """

    # We will implement logic in the next step
    # For now, just log input and fail intentionally

    print("Reconciliation event:", json.dumps(event))

    raise NotImplementedError("Reconciliation logic not implemented yet")