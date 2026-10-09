from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, UTC
import json
from typing import Any


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class TriggerActor:
    login: str
    avatar_url: str | None = None
    user_id: int | None = None


@dataclass(frozen=True)
class GitHubContext:
    urls: list[str]
    repo: str | None = None
    issue_number: int | None = None
    comment_id: int | None = None
    review_id: int | None = None
    review_comment_id: int | None = None
    commit_comment_id: int | None = None
    commit_sha: str | None = None
    target_kind: str | None = None
    workflow_run_id: int | None = None

    @property
    def work_key(self) -> str:
        if self.repo and self.issue_number:
            return f"{self.repo}#{self.issue_number}"
        if self.repo and self.commit_sha:
            return f"{self.repo}@{self.commit_sha[:12]}"
        if self.repo and self.workflow_run_id:
            return f"{self.repo}/actions/runs/{self.workflow_run_id}"
        return "unknown/repo#0"

    @property
    def short_url(self) -> str:
        return self.urls[0] if self.urls else "(sense URL)"

    @property
    def is_pull_request(self) -> bool:
        if not self.repo or not self.issue_number:
            return False
        if self.review_id or self.review_comment_id:
            return True
        expected = f"github.com/{self.repo}/pull/{self.issue_number}".lower()
        return expected in self.short_url.lower()

    @property
    def supports_commit_status(self) -> bool:
        return bool(
            self.repo
            and (
                self.is_pull_request
                or (
                    self.commit_sha
                    and self.target_kind in {"commit", "commit_comment"}
                )
            )
        )

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, value: str) -> "GitHubContext":
        return cls(**json.loads(value))


@dataclass(frozen=True)
class Notification:
    uid: int | None
    message_id: str
    subject: str
    from_addr: str
    body: str
    received_at: str = field(default_factory=utc_now)
    source_received_at: str | None = None
    auth: dict[str, bool] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Job:
    id: int
    work_key: str
    repo: str | None
    thread: int | None
    status: str
    action: str
    work_intent: str
    subject: str
    message_id: str
    uid: int | None
    context: GitHubContext
    trigger_actor: str | None = None
    trigger_actor_avatar_url: str | None = None
    attempts: int = 0
    coalesced_count: int = 0
    last_error: str | None = None
    locked_by: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    source_received_at: str | None = None
    terminal_outcome: str | None = None
    outcome_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
