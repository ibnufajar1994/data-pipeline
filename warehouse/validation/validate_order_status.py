def validate_order_status(status):
    return status in ["Completed", "Canceled"]