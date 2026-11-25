"""Tri automatique des emails de la boîte Outlook de l'utilisateur (on-behalf-of)."""

from datetime import timedelta
from typing import Any

import mistralai.workflows as workflows
from mistralai.workflows import Depends
from mistralai.workflows.plugins.mistralai.connectors import (
    ToolCallClient,
    connector,
    uses_connectors,
)
from pydantic import BaseModel, Field

from worker.workflows.tri_emails import (
    DEFAULT_MAX_CONCURRENT,
    Email,
    classer_email,
)

outlook = connector("outlook")


class TriEmailsOutlookInput(BaseModel):
    max_emails: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Nombre maximal d'emails lus dans la boîte de réception",
    )
    only_unread: bool = Field(
        default=False, description="Ne trier que les emails non lus"
    )
    query: str | None = Field(
        default=None, description="Recherche plein texte optionnelle"
    )
    max_concurrent: int = Field(
        default=DEFAULT_MAX_CONCURRENT,
        ge=1,
        le=100,
        description="Nombre maximal d'emails traités simultanément",
    )


class EmailOutlook(BaseModel):
    message_id: str
    web_link: str | None = None
    sender: str
    subject: str
    body: str


class EmailOutlookEnrichi(BaseModel):
    sender: str
    subject: str
    categorie: str
    resume: str
    priorite: int
    message_id: str
    web_link: str | None = None


class TriEmailsOutlookOutput(BaseModel):
    total: int
    emails: list[EmailOutlookEnrichi]


@workflows.activity(
    start_to_close_timeout=timedelta(minutes=2),
    retry_policy_max_attempts=3,
)
async def rechercher_emails_outlook(
    max_emails: int,
    only_unread: bool,
    query: str | None,
    client: ToolCallClient = Depends(outlook),
) -> list[str]:
    arguments: dict[str, Any] = {"folder_id": "inbox", "limit": max_emails}
    if only_unread:
        arguments["is_read"] = False
    if query:
        arguments["query"] = query
    result = await client.call_tool("search_outlook", arguments)
    items = result.get("results") or [] if isinstance(result, dict) else []
    message_ids = []
    for item in items:
        metadata = item.get("metadata") or {}
        message_id = metadata.get("message_id")
        if message_id:
            message_ids.append(message_id)
    return message_ids


@workflows.activity(
    start_to_close_timeout=timedelta(minutes=2),
    retry_policy_max_attempts=3,
)
async def ouvrir_email_outlook(
    message_id: str,
    client: ToolCallClient = Depends(outlook),
) -> EmailOutlook:
    result = await client.call_tool("open_outlook_email", {"message_id": message_id})
    return EmailOutlook(
        message_id=message_id,
        web_link=result.get("web_link"),
        sender=result.get("sender") or "",
        subject=result.get("subject") or "",
        body=result.get("body") or "",
    )


@workflows.workflow.define(name="tri_emails_outlook", on_behalf_of=True)
@uses_connectors(outlook)
class TriEmailsOutlookWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, input: TriEmailsOutlookInput) -> TriEmailsOutlookOutput:
        message_ids = await rechercher_emails_outlook(
            input.max_emails, input.only_unread, input.query
        )
        if not message_ids:
            return TriEmailsOutlookOutput(total=0, emails=[])
        ouverts = await workflows.execute_activities_in_parallel(
            activity=ouvrir_email_outlook,
            items=message_ids,
            max_concurrent_scheduled_tasks=input.max_concurrent,
        )
        emails = ouverts or []
        classifications = await workflows.execute_activities_in_parallel(
            activity=classer_email,
            items=[
                Email(sender=e.sender, subject=e.subject, body=e.body) for e in emails
            ],
            max_concurrent_scheduled_tasks=input.max_concurrent,
        )
        results = classifications or []
        return TriEmailsOutlookOutput(
            total=len(emails),
            emails=[
                EmailOutlookEnrichi(
                    sender=email.sender,
                    subject=email.subject,
                    **classification.model_dump(),
                    message_id=email.message_id,
                    web_link=email.web_link,
                )
                for email, classification in zip(emails, results)
            ],
        )
