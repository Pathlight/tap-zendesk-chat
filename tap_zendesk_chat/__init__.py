#!/usr/bin/env python3
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from requests.exceptions import HTTPError

from . import streams as streams_
from .context import Context
from .discover import discover_streams
from .http import Client
from .util import load_schema

REQUIRED_CONFIG_KEYS = ["start_date", "access_token"]
LOGGER = singer.get_logger()


def ensure_credentials_are_authorized(client):
    # The request will throw an exception if the credentials are not authorized
    client.request(streams_.DEPARTMENTS.tap_stream_id)


def is_account_endpoint_authorized(client):
    # The account endpoint is restricted to zopim accounts, meaning integrated
    # Zendesk accounts will get a 403 for this endpoint.
    try:
        client.request(streams_.ACCOUNT.tap_stream_id)
    except HTTPError as e:
        if e.response.status_code == 403:
            LOGGER.info(
                "Ignoring 403 from account endpoint - this must be an "
                "integrated Zendesk account. This endpoint will be excluded "
                "from discovery."
            )
            return False
        else:
            raise
    return True


def discover(config):
    client = Client(config)
    ensure_credentials_are_authorized(client)
    include_account_stream = is_account_endpoint_authorized(client)
    streams = [
        s for s in streams_.all_streams
        if include_account_stream or s.tap_stream_id != streams_.ACCOUNT.tap_stream_id
    ]
    catalog = {"streams": discover_streams(streams)}
    return Catalog.from_dict(catalog)


def output_schema(ctx, stream):
    stream_data = ctx.catalog.get_stream(stream.tap_stream_id)
    singer.write_schema(stream.tap_stream_id, stream_data.schema.to_dict(), stream.pk_fields)


def sync(ctx):
    currently_syncing = ctx.state.get("currently_syncing")
    start_idx = streams_.all_stream_ids.index(currently_syncing) \
        if currently_syncing else 0
    stream_ids_to_sync = [cs.tap_stream_id for cs in ctx.catalog.streams
                          if cs.is_selected()]
    streams = [s for s in streams_.all_streams[start_idx:]
               if s.tap_stream_id in stream_ids_to_sync]
    for stream in streams:
        # Output schemas of all streams to start so that we can output
        # records of any type when needed.
        output_schema(ctx, stream)
    for stream in streams:
        ctx.state["currently_syncing"] = stream.tap_stream_id
        ctx.write_state()
        stream.sync(ctx)
    ctx.state["currently_syncing"] = None
    ctx.write_state()


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discover(args.config).dump()
        print()
    else:
        catalog = Catalog.from_dict(args.properties) \
            if args.properties else discover(args.config)
        ctx = Context(args.config, args.state, catalog)
        sync(ctx)

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == "__main__":
    main()
