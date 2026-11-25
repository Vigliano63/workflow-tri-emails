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
    EmailOutlookEnrichi,
    TriEmailsOutlookWorkflow,
    _construire_rapport_html,
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


@workflows.activity(name="mon_adresse_outlook")
async def mock_mon_adresse_outlook() -> str:
    return "moi@exemple.fr"


@workflows.activity(name="creer_brouillon_rapport")
async def mock_creer_brouillon_rapport(
    destinataire: str, sujet: str, corps_html: str
) -> str:
    assert destinataire == "moi@exemple.fr"
    assert "Tri de vos emails" in corps_html
    return "draft-123"


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


def _enrichi(
    sujet: str, categorie: str, priorite: int, resume: str = "Res"
) -> EmailOutlookEnrichi:
    return EmailOutlookEnrichi(
        sender="a@x.fr",
        subject=sujet,
        categorie=categorie,
        resume=resume,
        priorite=priorite,
        message_id="m-1",
        web_link="https://outlook.live.com/mail/0/inbox/m-1",
    )


def test_rapport_html_trie_par_priorite_desc() -> None:
    rapport = _construire_rapport_html(
        [_enrichi("faible", "NEWSLETTER", 1), _enrichi("urgent", "URGENT", 5)]
    )
    assert rapport.index("urgent") < rapport.index("faible")
    assert "Tri de vos emails (2)" in rapport


def test_rapport_html_echappe_le_html_et_contient_les_champs() -> None:
    rapport = _construire_rapport_html(
        [_enrichi("<script>alert(1)</script>", "SPAM", 1, "Résumé & <b>gras</b>")]
    )
    assert "<script>alert(1)</script>" not in rapport
    assert "&lt;script&gt;" in rapport
    assert "Résumé &amp; &lt;b&gt;gras&lt;/b&gt;" in rapport
    assert "SPAM" in rapport
    assert 'href="https://outlook.live.com/mail/0/inbox/m-1"' in rapport
    assert "●○○○○" in rapport


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
            mock_mon_adresse_outlook,
            mock_creer_brouillon_rapport,
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
        assert result["brouillon_id"] == "draft-123"
        assert "Tri de vos emails (2)" in result["rapport_html"]


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
