"""Tri automatique des emails de la boîte Outlook de l'utilisateur (on-behalf-of)."""

import json
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


def _parse_tool_result(result: Any) -> Any:
    """Extrait la charge utile d'une réponse d'outil connecteur.

    `call_tool` renvoie le dump du CallToolResult MCP : la sortie de l'outil est
    un bloc `content` de type `text` contenant le JSON sérialisé.
    """
    if isinstance(result, dict):
        content = result.get("content")
        if isinstance(content, list):
            for block in content:
                if (
                    isinstance(block, dict)
                    and block.get("type") == "text"
                    and isinstance(block.get("text"), str)
                ):
                    try:
                        return json.loads(block["text"])
                    except json.JSONDecodeError:
                        return block["text"]
        return result
    return result


def _extraire_message_ids(data: Any) -> list[str]:
    results = data.get("results") if isinstance(data, dict) else None
    if isinstance(results, dict):
        items = list(results.values())
    elif isinstance(results, list):
        items = results
    else:
        items = []
    message_ids = []
    for item in items:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") or {}
        message_id = metadata.get("message_id") if isinstance(metadata, dict) else None
        if message_id:
            message_ids.append(message_id)
    return message_ids


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
    return _extraire_message_ids(_parse_tool_result(result))


@workflows.activity(
    start_to_close_timeout=timedelta(minutes=2),
    retry_policy_max_attempts=3,
)
async def ouvrir_email_outlook(
    message_id: str,
    client: ToolCallClient = Depends(outlook),
) -> EmailOutlook:
    result = await client.call_tool("open_outlook_email", {"message_id": message_id})
    data = _parse_tool_result(result)
    data = data if isinstance(data, dict) else {}
    return EmailOutlook(
        message_id=message_id,
        web_link=data.get("web_link"),
        sender=data.get("sender") or "",
        subject=data.get("subject") or "",
        body=data.get("body") or "",
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
