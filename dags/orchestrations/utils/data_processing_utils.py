def process_data_freshness_logs(ds, ti, **context):
    import logging

    import sys

    print(sys.path)

    logging.info(f"Processing data quality logs for {ds}")

    from .alerting_utils import send_msg_to_webex

    send_msg_to_webex(f"Data quality logs processed for {ds}")
