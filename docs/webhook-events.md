# GitHub webhook event inventory

This inventory tracks GitHub's documented webhook event names and the bridge's
Phase 1 support. It is an integration backlog as well as an operator reference.
GitHub may add events, so compare it with the official [webhook events and
payloads](https://docs.github.com/en/webhooks/webhook-events-and-payloads) when
planning a new integration. Last reviewed: 2026-10-02.

Support levels:

- **Canonical**: persisted with an immutable canonical event key and eligible
  for IMAP/webhook coverage comparison. It is observational in shadow mode and
  may enqueue in canary/primary mode when policy permits it.
- **Observed only**: a signed delivery can be persisted as `unsupported`, but
  no canonical key is derived.
- **Not selected**: operators should not subscribe to it in Phase 1.

| Support | GitHub event names |
| --- | --- |
| Canonical | `commit_comment`, `issue_comment`, `issues`, `pull_request`, `pull_request_review`, `pull_request_review_comment`, `workflow_run` |
| Observed only / not selected | `branch_protection_configuration`, `branch_protection_rule`, `check_run`, `check_suite`, `code_scanning_alert`, `create`, `custom_property`, `custom_property_values`, `delete`, `dependabot_alert`, `deploy_key`, `deployment`, `deployment_protection_rule`, `deployment_review`, `deployment_status`, `discussion`, `discussion_comment`, `fork`, `github_app_authorization`, `gollum`, `installation`, `installation_repositories`, `installation_target`, `issue_dependencies`, `issue_relates_to`, `label`, `marketplace_purchase`, `member`, `membership`, `merge_group`, `meta`, `milestone`, `org_block`, `organization`, `package`, `page_build`, `personal_access_token_request`, `ping`, `project`, `project_card`, `project_column`, `projects_v2`, `projects_v2_item`, `projects_v2_status_update`, `public`, `pull_request_review_thread`, `push`, `registry_package`, `release`, `repository`, `repository_advisory`, `repository_dispatch`, `repository_import`, `repository_ruleset`, `repository_vulnerability_alert`, `secret_scanning_alert`, `secret_scanning_alert_location`, `secret_scanning_scan`, `security_advisory`, `security_and_analysis`, `sponsorship`, `star`, `status`, `sub_issues`, `team`, `team_add`, `watch`, `workflow_dispatch`, `workflow_job` |

The inventory names event families, not every `action` value. Actions are
tracked separately because their delivery semantics differ. For comments and
reviews, `created`/`submitted` has the actionable canonical identity; `edited`
and other actions remain distinct observational identities and must not
retrigger work without an explicit Phase 2 policy decision. For `issues`, only
`assigned` is actionable when the payload assignee matches a configured bot
login. For `pull_request`, `assigned` and `review_requested` retain their
configured-bot checks, while `closed` is actionable only when `merged=true`;
that delivery becomes a read-only `sync_after_merge` job. Comment and review
deliveries must additionally be addressed to the bot, belong to a PR authored
by the bot, or target a PR/issue assigned to the bot. Being a reviewer,
subscriber, or email recipient is not authorization to work.
