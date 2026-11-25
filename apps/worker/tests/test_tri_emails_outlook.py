"""Tests du workflow `tri_emails_outlook` (connecteur Outlook et appel Mistral mockés)."""

import asyncio
import json
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
from worker.workflows.tri_emails_outlook import (
    EmailOutlook,
    TriEmailsOutlookWorkflow,
    _extraire_message_ids,
    _parse_tool_result,
)

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


def test_parse_tool_result_extrait_le_json_du_bloc_text() -> None:
    payload = {
        "results": {"a": {"metadata": {"message_id": "id-1"}}},
        "next_page_token": None,
    }
    dumped = {
        "content": [{"type": "text", "text": json.dumps(payload)}],
        "metadata": {},
    }
    assert _parse_tool_result(dumped) == payload


def test_parse_tool_result_garde_le_dict_direct() -> None:
    assert _parse_tool_result({"results": []}) == {"results": []}


def test_extraire_message_ids_resultats_en_dict() -> None:
    data = {
        "results": {
            "k1": {"metadata": {"message_id": "id-1"}},
            "k2": {"metadata": {"message_id": "id-2"}},
        }
    }
    assert sorted(_extraire_message_ids(data)) == ["id-1", "id-2"]


def test_extraire_message_ids_resultats_en_liste_et_vide() -> None:
    assert _extraire_message_ids(
        {"results": [{"metadata": {"message_id": "id-9"}}]}
    ) == ["id-9"]
    assert _extraire_message_ids({"results": {}}) == []
    assert _extraire_message_ids({}) == []


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
