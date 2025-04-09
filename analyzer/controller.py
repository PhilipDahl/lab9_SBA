from your_main_module import get_listing_event, get_transaction_event, get_event_stats
from flask import jsonify, request

def listing_route():
    index = request.args.get("index")
    if index is None or not index.isdigit():
        return { "message": "Invalid or missing 'index' parameter" }, 400
    return get_listing_event(int(index))

def transaction_route():
    index = request.args.get("index")
    if index is None or not index.isdigit():
        return { "message": "Invalid or missing 'index' parameter" }, 400
    return get_transaction_event(int(index))

def stats_route():
    return get_event_stats()
