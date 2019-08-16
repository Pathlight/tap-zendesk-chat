from datetime import datetime, timedelta
from pendulum import parse as dt_parse
from singer import metrics
import dataclasses
import json
import singer
import time
import uuid

LOGGER = singer.get_logger()


def clamp(min_val, val, max_val):
    """
    Clamp a number within a range

    >>> clamp(0, 0.5, 1)
    0.5
    >>> clamp(0, -1, 1)
    0
    >>> clamp(0, 10, 1)
    1
    """
    return min(max_val, max(min_val, val))


def break_into_intervals(days, hours, start_time: str, now: datetime):
    if days > 0:
        if hours > 0:
            delta = timedelta(days=days, hours=hours)
        else:
            delta = timedelta(days=days)
    elif hours > 0:
        delta = timedelta(hours=hours)

    start_dt = dt_parse(start_time)
    while start_dt < now:
        end_dt = min(start_dt + delta, now)
        yield start_dt, end_dt
        start_dt = end_dt


class Stream(object):
    """Information about and functions for syncing streams.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields"""
    def __init__(self, tap_stream_id, pk_fields):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields

    def metrics(self, page):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

    def format_response(self, response):
        return [response] if type(response) != list else response

    def write_page(self, page):
        """Formats a list of records in place and outputs the data to
        stdout."""
        singer.write_records(self.tap_stream_id, page)
        self.metrics(page)

    def is_selected(self, ctx):
        for context in ctx.catalog.streams:
            if context.tap_stream_id == self.tap_stream_id:
                return context.is_selected()
        return False

class Everything(Stream):
    def sync(self, ctx):
        self.write_page(ctx.client.request(self.tap_stream_id))


class Agents(Stream):
    def sync(self, ctx):
        since_id_offset = [self.tap_stream_id, "offset", "id"]
        since_id = ctx.bookmark(since_id_offset) or 0
        while True:
            params = {
                "since_id": since_id,
                "limit": ctx.config.get("agents_page_limit", 100),
            }
            page = ctx.client.request(self.tap_stream_id, params)
            if not page:
                break
            self.write_page(page)
            since_id = page[-1]["id"] + 1
            ctx.set_bookmark(since_id_offset, since_id)
            ctx.write_state()
        ctx.set_bookmark(since_id_offset, None)
        ctx.write_state()


class Engagements(Stream):
    def sync(self, ctx, engagements=None):
        if not engagements:
            # __init__.py calls sync on all streams, but this one is handled
            # within the Chats stream, so ignore this call.
            return
        self.write_page(engagements)


class Chats(Stream):
    LIMIT = 1000
    # How long to wait for more records in seconds
    WAIT_TIME = 60

    def _request(self, ctx, *args, **kwargs):
        return ctx.client.incremental_request(self.tap_stream_id, *args, **kwargs)

    def _start_request(self, ctx, start_dt: datetime):
        params = {
            "start_time": int(start_dt.timestamp()),
            "limit": self.LIMIT,
            "fields": "chats(*)",
        }
        return self._request(ctx, params=params)

    @dataclasses.dataclass
    class SubStream:
        """
        Since _sync actually has to manage two different object types
        (chat and offline_msg objects), create a small dataclass to store
        the state for each one (and perform stateful operations).

        Should be able to get rid of this class entirely when we get rid of
        `ts_bookmark_key`
        """

        tap_stream_id: str
        ctx: 'Context'

        # Deprecated fields. Won't be needed when we stop using
        # `ts_bookmark_key`.
        chat_type: str
        ts_field: str

        start_time: datetime = None

        @property
        def ts_bookmark_key(self):
            # This is an old bookmark key that we are migrating away from. We
            # read from it, but we don't write to it.
            return [self.tap_stream_id, self.chat_type + "." + self.ts_field]

        def update_start_date_bookmark(self):
            start_time = self.ctx.update_start_date_bookmark(self.ts_bookmark_key)
            self.start_time = dt_parse(start_time)
            return self.start_time

    def should_give_up(self, pull_id, ctx, record_counts):
        """
        Determine if it's time to give up making requests because the results
        are petering out. This is needed because zendesk chat's incremental
        api is strange and it doesn't tell you when it's done.

        The overall affect here is if we are consistently getting too few
        records, we give up and pick up the task later.

        NOTE: (PH) The way we are determining when we can fetch data right away
        and when we should wait to fetch data is at odds with the incremental
        api's documentation (which says when you get fewer than `limit`
        records, you should fetch without waiting, and if you have exactly
        `limit` records, you should wait). That advice doesn't make much
        sense to me and doesn't seem to match my observations of how their
        api works. What seems to happen is you get `limit` items until we
        reach the most recent records, at which point they trickle in. This
        method attempts to detect when it starts to trickle.
        """

        # If the average record count for the last
        # `incremental_give_up_num_requests` is < `incremental_give_up_ratio`
        # * self.LIMIT, then we give up for now.
        give_up_ratio = clamp(
            0, ctx.config.get('incremental_give_up_ratio', 0.05), 1)
        give_up_num_requests = ctx.config.get('incremental_give_up_num_requests', 5)

        if len(record_counts) < give_up_num_requests:
            return False
        # Truncate list to get rid of values we will no longer use.
        while len(record_counts) > give_up_num_requests:
            record_counts.pop(0)

        give_up_threshold = self.LIMIT * give_up_ratio
        avg_counts = sum(record_counts) / len(record_counts)
        give_up = give_up_threshold > avg_counts
        LOGGER.info(
            '%s: give_up_threshold=%s, avg_counts=%s, give_up=%s',
            pull_id, give_up_threshold, avg_counts, give_up)
        return give_up

    def _sync(self, ctx, *, full_sync):
        """
        Pulls data from the incremental chats api and manage sending the
        offline messages to the offline_msg stream and the chat messages to
        the chat stream.

        full_sync is a boolean indicating whether or not to pull all chats
        based on the "start_date" in the config. When this is true, all
        bookmarks for this chat type will be ignored.

        TODO add a way to sync engagements as well.
        """
        pull_id = uuid.uuid4()

        chats = self.SubStream(
            ctx=ctx, tap_stream_id=self.tap_stream_id, chat_type='chat',
            ts_field='end_timestamp')
        offline_msgs = self.SubStream(
            ctx=ctx, tap_stream_id=self.tap_stream_id, chat_type='offline_msg',
            ts_field='timestamp')
        substreams = [chats, offline_msgs]

        url_offset_key = [self.tap_stream_id, "offset", "chats-incremental.next_url"]
        ts_bookmark_key = [self.tap_stream_id, "chats-incremental.end_time"]

        engagements = None
        for stream in all_streams:
            if stream.tap_stream_id == 'engagements' and stream.is_selected(ctx):
                engagements = stream
                break

        if full_sync:
            for stream in substreams:
                stream.set_ts_bookmark(None)
        for stream in substreams:
            stream.update_start_date_bookmark()
        start_time = dt_parse(ctx.update_start_date_bookmark(ts_bookmark_key))
        if not start_time:
            start_time = min(s.start_time for s in substreams)
        max_bookmark = start_time

        end_dt = ctx.now
        # This will be moved forward with each request
        end_time = start_time
        log_attrs = ["start_dt=%s" % start_time, "end_dt=%s" % end_dt]

        resp = None
        next_url = None
        record_counts = []
        while end_time < end_dt:
            if next_url:
                resp = self._request(ctx, url=next_url)
            else:
                resp = self._start_request(ctx, start_time)

            record_counts.append(resp['count'])

            next_url = resp["next_page"]
            ctx.set_bookmark(url_offset_key, next_url)

            LOGGER.info('{} Request completed: {}'.format(pull_id, ",".join(log_attrs + ['count=%s' % resp['count']])))

            # Allows us to break out of the loop when we go past our desired endpoint
            end_time = datetime.utcfromtimestamp(resp["end_time"])
            max_bookmark = max(max_bookmark, end_time)

            chats = resp['chats']
            for chat in chats:
                engagements_data = chat.pop('engagements', None)
                if engagements and engagements_data:
                    for engagement in engagements_data:
                        engagement['chat_id'] = chat['id']
                    engagements.sync(ctx, engagements_data)

            self.write_page(chats)
            ctx.set_bookmark(ts_bookmark_key, max_bookmark)
            ctx.write_state()

            if not next_url:
                LOGGER.info('{} Finished syncing: {}'.format(pull_id, ",".join(log_attrs)))
                break

            if self.should_give_up(pull_id, ctx, record_counts):
                LOGGER.info('{} Got too few results; giving up: {}'.format(pull_id, ",".join(log_attrs)))
                break

    def _should_run_full_sync(self, ctx):
        sync_days = ctx.config.get("chats_full_sync_days")
        if sync_days:
            last_sync = ctx.state.get("chats_last_full_sync")
            if not last_sync:
                LOGGER.info("Running full sync of chats: no last sync time")
                return True
            next_sync = dt_parse(last_sync) + timedelta(days=sync_days)
            if next_sync <= ctx.now:
                LOGGER.info("Running full sync of chats: "
                            "last sync was {}, configured to run every {} days"
                            .format(last_sync, sync_days))
                return True
        return False

    def sync(self, ctx):
        full_sync = self._should_run_full_sync(ctx)
        self._sync(ctx, full_sync=full_sync)
        if full_sync:
            ctx.state["chats_last_full_sync"] = ctx.now.isoformat()
            ctx.write_state()


class Triggers(Stream):
    def sync(self, ctx):
        page = ctx.client.request(self.tap_stream_id)
        for trigger in page:
            definition = trigger["definition"]
            for k in ["condition", "actions"]:
                definition[k] = json.dumps(definition[k])
        self.write_page(page)


class Bans(Stream):
    def sync(self, ctx):
        response = ctx.client.request(self.tap_stream_id)
        page = response["visitor"] + response["ip_address"]
        self.write_page(page)


class Account(Stream):
    def sync(self, ctx):
        # The account endpoint returns a single item, so we have to wrap it in
        # a list to write a "page"
        self.write_page([ctx.client.request(self.tap_stream_id)])

DEPARTMENTS = Everything("departments", ["id"])
ACCOUNT = Account("account", ["account_key"])
all_streams = [
    Agents("agents", ["id"]),
    Chats("chats", ["id"]),
    Engagements("engagements", ["id"]),
    Everything("shortcuts", ["name"]),
    Triggers("triggers", ["id"]),
    Bans("bans", ["id"]),
    DEPARTMENTS,
    Everything("goals", ["id"]),
    ACCOUNT,
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
