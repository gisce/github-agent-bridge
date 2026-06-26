import imaplib

from github_agent_bridge import reader


class FakeQueue:
    def __init__(self):
        self.state = {}
        self.enqueued = []

    def get_state(self, key, default=""):
        return self.state.get(key, default)

    def set_state(self, key, value):
        self.state[key] = value

    def enqueue(self, notification, policy):
        self.enqueued.append(notification)
        return None, "enqueued"


class FetchErrorImap:
    logged_out = False

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def login(self, username, password):
        return "OK", []

    def select(self, mailbox):
        return "OK", []

    def uid(self, command, *args):
        if command == "search":
            return "OK", [b"42"]
        if command == "fetch":
            raise imaplib.IMAP4.error(b"Lookup failed 423d73b8af312-310163d197dmb4381437a26")
        raise AssertionError(command)

    def logout(self):
        type(self).logged_out = True


class FetchNoImap(FetchErrorImap):
    def uid(self, command, *args):
        if command == "search":
            return "OK", [b"43"]
        if command == "fetch":
            return "NO", [b"Lookup failed 423d73b8af312-310163d197dmb4381437a26"]
        raise AssertionError(command)


class FetchTemporaryNoImap(FetchErrorImap):
    def uid(self, command, *args):
        if command == "search":
            return "OK", [b"44"]
        if command == "fetch":
            return "NO", [b"Temporary unavailable"]
        raise AssertionError(command)


def test_reader_records_and_skips_imap_fetch_lookup_error(monkeypatch):
    queue = FakeQueue()
    monkeypatch.setattr(reader.imaplib, "IMAP4_SSL", FetchErrorImap)

    count = reader.ImapReader(
        reader.ImapConfig("imap.example.com", 993, "bot@example.com", "secret"),
        queue,
        policy=object(),
    ).fetch_once()

    assert count == 0
    assert queue.enqueued == []
    assert queue.state["last_uid"] == "42"
    assert queue.state["last_imap_fetch_error_uid"] == "42"
    assert "Lookup failed" in queue.state["last_imap_fetch_error"]
    assert FetchErrorImap.logged_out is True


def test_reader_records_and_skips_failed_imap_fetch_status(monkeypatch):
    queue = FakeQueue()
    monkeypatch.setattr(reader.imaplib, "IMAP4_SSL", FetchNoImap)

    count = reader.ImapReader(
        reader.ImapConfig("imap.example.com", 993, "bot@example.com", "secret"),
        queue,
        policy=object(),
    ).fetch_once()

    assert count == 0
    assert queue.enqueued == []
    assert queue.state["last_uid"] == "43"
    assert queue.state["last_imap_fetch_error_uid"] == "43"
    assert queue.state["last_imap_fetch_error"].startswith("NO:")


def test_reader_does_not_advance_last_uid_for_non_lookup_fetch_failure(monkeypatch):
    queue = FakeQueue()
    monkeypatch.setattr(reader.imaplib, "IMAP4_SSL", FetchTemporaryNoImap)

    count = reader.ImapReader(
        reader.ImapConfig("imap.example.com", 993, "bot@example.com", "secret"),
        queue,
        policy=object(),
    ).fetch_once()

    assert count == 0
    assert queue.enqueued == []
    assert "last_uid" not in queue.state
    assert queue.state["last_imap_fetch_error_uid"] == "44"
    assert "Temporary unavailable" in queue.state["last_imap_fetch_error"]
