import os
import json
import singer

from .util import get_abs_path


def discover_streams(all_streams):
    streams = []
    for s in all_streams:
        schema = singer.resolve_schema_references(s.load_schema())
        streams.append({
            'stream': s.tap_stream_id,
            'tap_stream_id': s.tap_stream_id,
            'schema': schema,
            'metadata': s.load_metadata()
        })
    return streams
