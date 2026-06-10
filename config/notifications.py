"""
Shared DAG-completion email notifications.

Sends an email when a DAG run finishes (both on success and on failure) via the
Airflow SMTP provider. The SMTP server itself is configured at the infrastructure
level (``smtp_default`` connection or ``AIRFLOW__SMTP__*`` settings), so this code
is provider-agnostic — it works the same against a local Postfix, an internal
relay, or a capture tool like Mailpit.

Recipients are read at runtime from the environment (``.env`` / process env), never
hardcoded:

- ``ALERT_EMAIL_TO``   — comma-separated recipient list (required to enable emails)
- ``ALERT_EMAIL_FROM`` — optional sender override (defaults to the SMTP config)

The factory is **defensive on purpose**: if no recipient is configured or the SMTP
provider is unavailable, it returns no callbacks so DAGs still load cleanly (the
DAG-integrity gate must never break because email is unset).
"""

from __future__ import annotations

from typing import Any

from config.env import get_env


def _recipients() -> list[str]:
    """Parse ``ALERT_EMAIL_TO`` into a clean list of addresses."""
    return [addr.strip() for addr in get_env("ALERT_EMAIL_TO").split(",") if addr.strip()]


def _build_notifier(state_label: str) -> Any | None:
    """
    Build an ``SmtpNotifier`` for a given DAG-run outcome, or ``None`` if email is
    not configured / the SMTP provider is missing.
    """
    recipients = _recipients()
    if not recipients:
        return None

    try:
        from airflow.providers.smtp.notifications.smtp import SmtpNotifier
    except Exception:
        return None

    subject = f"[Airflow] {{{{ dag.dag_id }}}} — {state_label}"
    html_content = (
        f"<p>DAG <b>{{{{ dag.dag_id }}}}</b> {state_label}.</p>"
        "<ul>"
        "<li>Run: {{ run_id }}</li>"
        "<li>Logical date: {{ logical_date }}</li>"
        "</ul>"
    )

    kwargs: dict[str, Any] = {
        "to": recipients,
        "subject": subject,
        "html_content": html_content,
    }
    from_email = get_env("ALERT_EMAIL_FROM")
    if from_email:
        kwargs["from_email"] = from_email

    return SmtpNotifier(**kwargs)


def completion_callbacks() -> dict[str, Any]:
    """
    Return DAG-level callbacks that email when a run finishes (success and failure).

    Spread into a ``DAG(...)`` constructor with ``**completion_callbacks()``. Returns
    an empty dict when email is not configured, so it is safe to call unconditionally.

    Returns
    -------
    dict
        ``{"on_success_callback": ..., "on_failure_callback": ...}`` or ``{}``.
    """
    callbacks: dict[str, Any] = {}
    success = _build_notifier("finished successfully")
    failure = _build_notifier("FAILED")
    if success is not None:
        callbacks["on_success_callback"] = success
    if failure is not None:
        callbacks["on_failure_callback"] = failure
    return callbacks
