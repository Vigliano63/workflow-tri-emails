"""Tests du workflow `tri_emails_outlook` (connecteur Outlook et appel Mistral mockés)."""

import asyncio
from datetime import timedelta
from typing import Any, Type, TypeVar

import mistralai.workflows as workflows
import pytest
from mistralai.workflows.core.execution.concurrency import ParallelExecutionWorkflow
from mistralai.workflows.core.execution.concurrency._concurrency_workflow import (
    OnBehalfOfParallelExecutionWorkflow,
)
from mistralai.workflows.testing import create_test_worker

from worker.workflows import tri_emails
from worker.workflows.tri_emails_outlook import EmailOutlook, TriEmailsOutlookWorkflow

EXECUTION_TIMEOUT = timedelta(seconds=30)

T = TypeVar("T")


async def _fake_chat_parse_to_model(model_class: Type[T], request: Any) -> T:
    return model_class(categorie="INFORMATION", resume="Résumé de test.", priorite=2)


@pytest.fixture
def mock_chat_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tri_emails, "chat_parse_to_model", _fake_chat_parse_to_model)


@workflows.activity(name="rechercher_emails_outlook")
async def mock_rechercher_emails_outlook(
    max_emails: int, only_unread: bool, query: str | None
) -> list[str]:
    return ["msg-1", "msg-2"]


@workflows.activity(name="ouvrir_email_outlook")
async def mock_ouvrir_email_outlook(message_id: str) -> EmailOutlook:
    return EmailOutlook(
        message_id=message_id,
        sender=f"expediteur-{message_id}@exemple.fr",
        subject=f"Objet {message_id}",
        body=f"Corps du message {message_id}",
        web_link=f"https://outlook.live.com/mail/0/inbox/{message_id}",
    )


async def test_tri_emails_outlook_lit_et_classe(
    temporal_env: Any, mock_chat_parse: None
) -> None:
    async with create_test_worker(
        temporal_env,
        workflows=[
            TriEmailsOutlookWorkflow,
            ParallelExecutionWorkflow,
            OnBehalfOfParallelExecutionWorkflow,
        ],
        activities=[
            mock_rechercher_emails_outlook,
            mock_ouvrir_email_outlook,
            tri_emails.classer_email,
        ],
    ):
        handle = await temporal_env.client.start_workflow(
            "tri_emails_outlook",
            {"max_emails": 2, "max_concurrent": 2},
            id="test-tri-emails-outlook",
            task_queue="test-task-queue",
            execution_timeout=EXECUTION_TIMEOUT,
        )
        result = await asyncio.wait_for(handle.result(), timeout=60)
        assert result["total"] == 2
        assert len(result["emails"]) == 2
        for email in result["emails"]:
            assert email["sender"].startswith("expediteur-msg-")
            assert email["subject"].startswith("Objet msg-")
            assert email["categorie"] == "INFORMATION"
            assert email["resume"] == "Résumé de test."
            assert email["priorite"] == 2
            assert email["message_id"].startswith("msg-")
            assert email["web_link"].startswith("https://outlook.live.com/")


async def test_tri_emails_outlook_boite_vide(
    temporal_env: Any, mock_chat_parse: None
) -> None:
    @workflows.activity(name="rechercher_emails_outlook")
    async def mock_rechercher_vide(
        max_emails: int, only_unread: bool, query: str | None
    ) -> list[str]:
        return []

    async with create_test_worker(
        temporal_env,
        workflows=[
            TriEmailsOutlookWorkflow,
            ParallelExecutionWorkflow,
            OnBehalfOfParallelExecutionWorkflow,
        ],
        activities=[
            mock_rechercher_vide,
            mock_ouvrir_email_outlook,
            tri_emails.classer_email,
        ],
    ):
        handle = await temporal_env.client.start_workflow(
            "tri_emails_outlook",
            {"max_emails": 5},
            id="test-tri-emails-outlook-vide",
            task_queue="test-task-queue",
            execution_timeout=EXECUTION_TIMEOUT,
        )
        result = await asyncio.wait_for(handle.result(), timeout=60)
        assert result["total"] == 0
        assert result["emails"] == []
