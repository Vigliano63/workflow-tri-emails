"""Tests du workflow `tri_emails` avec l'appel au modèle Mistral mocké."""

import asyncio
from datetime import timedelta
from typing import Any, Type, TypeVar

import pytest
from mistralai.workflows.core.execution.concurrency import ParallelExecutionWorkflow
from mistralai.workflows.plugins.mistralai import ChatCompletionRequest
from mistralai.workflows.testing import create_test_worker

from worker.workflows import tri_emails
from worker.workflows.tri_emails import Email, TriEmailsWorkflow, classer_email

EXECUTION_TIMEOUT = timedelta(seconds=30)

T = TypeVar("T")

EMAILS = [
    Email(
        sender="cto@entreprise.fr",
        subject="Incident production",
        body="La base de données de production est indisponible depuis 5 minutes.",
    ),
    Email(
        sender="news@boutique.fr",
        subject="Les soldes arrivent",
        body="Découvrez nos promotions de la semaine dans notre catalogue.",
    ),
]


async def _fake_chat_parse_to_model(
    model_class: Type[T], request: ChatCompletionRequest
) -> T:
    content = request.messages[-1].content
    text = content if isinstance(content, str) else str(content)
    if "Incident" in text:
        return model_class(
            categorie="URGENT", resume="Incident de production en cours.", priorite=5
        )
    return model_class(
        categorie="NEWSLETTER", resume="Email commercial de soldes.", priorite=1
    )


@pytest.fixture
def mock_chat_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tri_emails, "chat_parse_to_model", _fake_chat_parse_to_model)


async def test_tri_emails_enrichit_chaque_email(
    temporal_env: Any, mock_chat_parse: None
) -> None:
    async with create_test_worker(
        temporal_env,
        workflows=[TriEmailsWorkflow, ParallelExecutionWorkflow],
        activities=[classer_email],
    ):
        handle = await temporal_env.client.start_workflow(
            "tri_emails",
            {"emails": [email.model_dump() for email in EMAILS], "max_concurrent": 2},
            id="test-tri-emails",
            task_queue="test-task-queue",
            execution_timeout=EXECUTION_TIMEOUT,
        )
        result = await asyncio.wait_for(handle.result(), timeout=60)
        assert len(result["emails"]) == 2
        by_sender = {email["sender"]: email for email in result["emails"]}
        urgent = by_sender["cto@entreprise.fr"]
        assert urgent["subject"] == "Incident production"
        assert urgent["categorie"] == "URGENT"
        assert urgent["resume"] == "Incident de production en cours."
        assert urgent["priorite"] == 5
        news = by_sender["news@boutique.fr"]
        assert news["subject"] == "Les soldes arrivent"
        assert news["categorie"] == "NEWSLETTER"
        assert news["priorite"] == 1


async def test_tri_emails_liste_vide(temporal_env: Any) -> None:
    async with create_test_worker(
        temporal_env,
        workflows=[TriEmailsWorkflow, ParallelExecutionWorkflow],
        activities=[classer_email],
    ):
        handle = await temporal_env.client.start_workflow(
            "tri_emails",
            {"emails": []},
            id="test-tri-emails-vide",
            task_queue="test-task-queue",
            execution_timeout=EXECUTION_TIMEOUT,
        )
        result = await asyncio.wait_for(handle.result(), timeout=60)
        assert result["emails"] == []
