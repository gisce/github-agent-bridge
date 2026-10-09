import imaplib
import sqlite3

import pytest

import github_agent_bridge.reader as reader_module
from github_agent_bridge.reader import ImapConfig, ImapReader
from github_agent_bridge.policy import Policy
from github_agent_bridge.queue import JobQueue


class QueueStub:
    def get_state(self, key, default=None):
        return default


class AbortOnSelect:
    def __init__(self, *args):
        self.logged_out = False

    def login(self, username, password):
        return "OK", []

    def select(self, mailbox):
        raise imaplib.IMAP4.abort("command: SELECT => socket error: EOF")

    def logout(self):
        self.logged_out = True


class EmptyMailbox:
    def __init__(self, *args):
        self.logged_out = False

    def login(self, username, password):
        return "OK", []

    def select(self, mailbox):
        return "OK", []

    def uid(self, command, *args):
        assert command == "search"
        return "OK", [b""]

    def logout(self):
        self.logged_out = True


class MailboxWithMessages:
    def __init__(self, messages, internaldates=None):
        self.messages = messages
        self.internaldates = internaldates or {}
        self.logged_out = False
        self.stores = []

    def login(self, username, password):
        return "OK", []

    def select(self, mailbox):
        return "OK", []

    def uid(self, command, *args):
        if command == "search":
            return "OK", [b" ".join(str(uid).encode("ascii") for uid in self.messages)]
        if command == "fetch":
            uid = int(args[0])
            internaldate = self.internaldates.get(uid)
            metadata = (
                f'{uid} (UID {uid} INTERNALDATE "{internaldate}" RFC822'.encode()
                if internaldate
                else None
            )
            return "OK", [(metadata, self.messages[uid])]
        if command == "store":
            self.stores.append(args)
            return "OK", []
        raise AssertionError(f"unexpected IMAP uid command {command}")

    def logout(self):
        self.logged_out = True


def make_reader():
    config = ImapConfig("imap.example.com", 993, "bot@example.com", "secret")
    return ImapReader(config, QueueStub(), object())


def test_fetch_once_reconnects_with_backoff_after_imap_aborts(monkeypatch):
    connections = [AbortOnSelect(), AbortOnSelect(), EmptyMailbox()]
    sleeps = []

    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *args: connections.pop(0))
    monkeypatch.setattr(reader_module.time, "sleep", sleeps.append)

    assert make_reader().fetch_once() == 0
    assert connections == []
    assert sleeps == [1.0, 2.0]


def test_fetch_once_raises_after_all_imap_retries(monkeypatch):
    connections = [AbortOnSelect(), AbortOnSelect(), AbortOnSelect()]
    sleeps = []

    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *args: connections.pop(0))
    monkeypatch.setattr(reader_module.time, "sleep", sleeps.append)

    with pytest.raises(imaplib.IMAP4.abort, match="socket error: EOF"):
        make_reader().fetch_once()

    assert connections == []
    assert sleeps == [1.0, 2.0]


def github_message(message_id, body):
    return (
        "From: GitHub <notifications@github.com>\r\n"
        f"Message-ID: {message_id}\r\n"
        "Subject: Re: [gisce/erp] issue\r\n"
        "Authentication-Results: mx.example; spf=pass dkim=pass dmarc=pass\r\n"
        "\r\n"
        f"{body}\r\n"
    ).encode("utf-8")


def test_fetch_once_quarantines_poison_github_notification_and_continues(monkeypatch, tmp_path):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    mailbox = MailboxWithMessages({
        1: github_message("<bad@github.com>", "BROKEN"),
        2: github_message(
            "<good@github.com>",
            "@pilipilisbot https://github.com/gisce/erp/issues/42#issuecomment-99",
        ),
    })

    def extract_context_or_fail(body):
        if "BROKEN" in body:
            raise ValueError("missing GitHub context")
        from github_agent_bridge.parser import extract_github_context

        return extract_github_context(body)

    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *args: mailbox)
    monkeypatch.setattr("github_agent_bridge.queue.extract_github_context", extract_context_or_fail)
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)

    reader = ImapReader(
        ImapConfig("imap.example.com", 993, "bot@example.com", "secret"),
        queue,
        Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"}),
    )

    assert reader.fetch_once() == 2
    assert queue.get_state("last_uid") == "2"
    assert queue.stats()["pending"] == 1
    with sqlite3.connect(db) as con:
        con.row_factory = sqlite3.Row
        quarantine = con.execute("SELECT * FROM quarantined_notifications").fetchone()
        job = con.execute("SELECT * FROM jobs").fetchone()

    assert quarantine["uid"] == 1
    assert quarantine["message_id"] == "<bad@github.com>"
    assert quarantine["reason"] == "ingestion_error"
    assert "missing GitHub context" in quarantine["error"]
    assert "BROKEN" in quarantine["body_excerpt"]
    assert job["uid"] == 2


def test_fetch_once_persists_imap_internaldate_as_source_received_at(
    monkeypatch, tmp_path
):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    mailbox = MailboxWithMessages(
        {
            1: github_message(
                "<internaldate@github.com>",
                "@pilipilisbot https://github.com/gisce/erp/issues/42#issuecomment-99",
            ),
        },
        internaldates={1: "08-Oct-2026 23:30:00 +0200"},
    )
    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *args: mailbox)
    monkeypatch.setattr(
        "github_agent_bridge.actors.github_actor_details_for_context",
        lambda ctx, *, gh_bin="gh": None,
    )

    reader = ImapReader(
        ImapConfig("imap.example.com", 993, "bot@example.com", "secret"),
        queue,
        Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"}),
    )

    assert reader.fetch_once() == 1
    with sqlite3.connect(db) as con:
        source_received_at = con.execute(
            "SELECT source_received_at FROM jobs"
        ).fetchone()[0]

    assert source_received_at == "2026-10-08T21:30:00Z"


def test_fetch_once_retries_transient_enqueue_storage_failure(monkeypatch, tmp_path):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    mailbox = MailboxWithMessages({
        1: github_message(
            "<retry@github.com>",
            "@pilipilisbot https://github.com/gisce/erp/issues/42#issuecomment-99",
        ),
    })
    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *args: mailbox)
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)

    queue.database.timeout_seconds = 0.01
    queue.database.busy_timeout_ms = 10
    locker = sqlite3.connect(db, isolation_level=None)
    locker.execute("BEGIN IMMEDIATE")
    released = {"value": False}
    sleeps = []

    def release_lock(delay):
        sleeps.append(delay)
        locker.commit()
        locker.close()
        released["value"] = True

    monkeypatch.setattr(reader_module.time, "sleep", release_lock)

    reader = ImapReader(
        ImapConfig("imap.example.com", 993, "bot@example.com", "secret"),
        queue,
        Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"}),
    )

    try:
        assert reader.fetch_once() == 1
    finally:
        if not released["value"]:
            locker.rollback()
            locker.close()

    assert sleeps == [1.0]
    assert queue.get_state("last_uid") == "1"
    assert queue.stats()["pending"] == 1
    with sqlite3.connect(db) as con:
        quarantined_count = con.execute("SELECT COUNT(*) FROM quarantined_notifications").fetchone()[0]

    assert quarantined_count == 0


def test_fetch_once_retries_state_advance_without_repeating_imap_effects(
    monkeypatch, tmp_path
):
    db = tmp_path / "bridge.sqlite3"
    queue = JobQueue(db)
    mailbox = MailboxWithMessages({
        1: github_message(
            "<retry-state@github.com>",
            "@pilipilisbot https://github.com/gisce/erp/issues/42#issuecomment-99",
        ),
    })
    original_set_state = queue.set_state
    attempts = {"count": 0}
    sleeps = []

    def fail_once_then_set_state(key, value):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return original_set_state(key, value)

    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *args: mailbox)
    monkeypatch.setattr(queue, "set_state", fail_once_then_set_state)
    monkeypatch.setattr(reader_module.time, "sleep", sleeps.append)
    monkeypatch.setattr("github_agent_bridge.actors.github_actor_details_for_context", lambda ctx, *, gh_bin="gh": None)

    reader = ImapReader(
        ImapConfig("imap.example.com", 993, "bot@example.com", "secret"),
        queue,
        Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"}),
        mark_seen=True,
    )

    assert reader.fetch_once() == 1
    assert attempts["count"] == 2
    assert sleeps == [1.0]
    assert len(mailbox.stores) == 1
    assert queue.get_state("last_uid") == "1"
    assert queue.stats()["pending"] == 1


def test_fetch_once_does_not_retry_non_contention_storage_failure(
    monkeypatch, tmp_path
):
    queue = JobQueue(tmp_path / "bridge.sqlite3")
    mailbox = MailboxWithMessages({
        1: github_message(
            "<broken-storage@github.com>",
            "@pilipilisbot https://github.com/gisce/erp/issues/42#issuecomment-99",
        ),
    })
    sleeps = []

    def fail_ingest(*args, **kwargs):
        raise sqlite3.OperationalError("no such table: jobs")

    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *args: mailbox)
    monkeypatch.setattr(queue, "ingest", fail_ingest)
    monkeypatch.setattr(reader_module.time, "sleep", sleeps.append)

    reader = ImapReader(
        ImapConfig("imap.example.com", 993, "bot@example.com", "secret"),
        queue,
        Policy(trusted_orgs={"gisce"}, bot_logins={"pilipilisbot"}),
    )

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        reader.fetch_once()

    assert sleeps == []
    assert queue.get_state("last_uid", "0") == "0"
