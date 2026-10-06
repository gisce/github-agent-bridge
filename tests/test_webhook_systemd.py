from pathlib import Path

from github_agent_bridge.backend import build_parser


ROOT = Path(__file__).resolve().parents[1]


def test_webhook_cli_accepts_inherited_socket_fd():
    args = build_parser(ingress=True).parse_args(["--fd", "3"])

    assert args.fd == 3
    assert args.port == 8766


def test_webhook_systemd_service_consumes_socket_activation_fd():
    service = (ROOT / "systemd/github-agent-bridge-webhook.service").read_text()
    socket = (ROOT / "systemd/github-agent-bridge-webhook.socket").read_text()

    assert "Sockets=github-agent-bridge-webhook.socket" in service
    assert "github-agent-bridge-webhook --fd 3" in service
    assert "RestartSec=500ms" in service
    assert "ListenStream=127.0.0.1:8766" in socket
    assert "Backlog=4096" in socket


def test_nginx_routes_webhook_to_dedicated_ingress():
    nginx = (ROOT / "docs/nginx-dashboard.conf").read_text()

    webhook_location = nginx.index("location = /api/webhooks/github")
    dashboard_location = nginx.index("location / {")
    assert webhook_location < dashboard_location
    assert "proxy_pass http://127.0.0.1:8766;" in nginx[webhook_location:dashboard_location]
