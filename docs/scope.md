# Scope

`github-agent-bridge` is intentionally GitHub-only.

## Boundary

```mermaid
flowchart TD
    G[GitHub] --> W[Signed webhook]
    G --> A[Notification email]
    A --> B[Generic inbox worker]
    A --> C[GitHub Agent Bridge IMAP input]
    W --> C

    B --> B1[ordinary email triage]
    B --> B2[calendar invites]
    B --> B3[reminders/status emails]

    C --> C1[normalize GitHub event]
    C --> C2[enqueue durable GitHub job]
    C --> C3[coalesce per owner/repo#number]
    C --> C4[react 👀]
    C --> C5[dispatch OpenClaw agent work]
```

## Rules

| Rule | Reason |
| --- | --- |
| Do not become a generic inbox assistant. | Keeps policy, safety, and failure modes narrow. |
| Do not mutate non-GitHub mail. | Prevents accidental mailbox side effects. |
| Keep generic email logic elsewhere. | Calendar/status/personal triage has different semantics. |
| Accept delegated GitHub messages from another worker. | Allows a future single IMAP owner if needed. |

## Mailbox ownership

The bridge may scan the same mailbox with its own high-water cursor, but it must only mutate GitHub notification messages.

If a generic worker becomes the sole IMAP owner later, it can call `gab enqueue-json` for GitHub messages instead of letting the bridge read IMAP directly.
