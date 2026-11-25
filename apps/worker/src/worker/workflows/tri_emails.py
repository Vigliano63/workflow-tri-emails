"""Tri d'emails : classe chaque email avec ``mistral-small-latest`` en parallèle."""

from datetime import timedelta
from typing import Literal

import mistralai.workflows as workflows
from mistralai.workflows.plugins.mistralai import (
    ChatCompletionRequest,
    SystemMessage,
    UserMessage,
    chat_parse_to_model,
)
from pydantic import BaseModel, Field

MODEL = "mistral-small-latest"
DEFAULT_MAX_CONCURRENT = 10

SYSTEM_PROMPT = """Tu es un assistant de tri d'emails professionnel.

Analyse l'email fourni (expéditeur, objet, corps) et réponds en JSON structuré avec
exactement trois champs :

- categorie : EXACTEMENT une des cinq valeurs suivantes, sans autre texte :
  - URGENT : incident de sécurité, panne, alerte critique ou échéance imminente
    mettant en danger des personnes, des systèmes ou l'activité
  - ACTION_REQUISE : l'expéditeur demande explicitement une action, une décision
    ou une réponse de la part du destinataire
  - INFORMATION : email purement informatif, aucune action requise
  - NEWSLETTER : communication marketing, annonce produit ou contenu périodique
    envoyé en masse
  - SPAM : email non sollicité, frauduleux, tentative de phishing ou indésirable
- resume : résumé de l'email en UNE seule phrase, en français
- priorite : entier de 1 (très faible) à 5 (critique), reflétant l'urgence réelle
"""


class Email(BaseModel):
    sender: str
    subject: str
    body: str


class TriEmailsInput(BaseModel):
    emails: list[Email]
    max_concurrent: int = Field(
        default=DEFAULT_MAX_CONCURRENT,
        ge=1,
        le=100,
        description="Nombre maximal d'emails classés simultanément",
    )


class EmailClassification(BaseModel):
    categorie: Literal["URGENT", "ACTION_REQUISE", "INFORMATION", "NEWSLETTER", "SPAM"]
    resume: str
    priorite: int = Field(ge=1, le=5)


class EmailEnrichi(BaseModel):
    sender: str
    subject: str
    categorie: str
    resume: str
    priorite: int


class TriEmailsOutput(BaseModel):
    emails: list[EmailEnrichi]


@workflows.activity(
    start_to_close_timeout=timedelta(minutes=2),
    retry_policy_max_attempts=3,
)
async def classer_email(email: Email) -> EmailClassification:
    request = ChatCompletionRequest(
        model=MODEL,
        temperature=0.0,
        messages=[
            SystemMessage(content=SYSTEM_PROMPT),
            UserMessage(
                content=(
                    f"Expéditeur : {email.sender}\n"
                    f"Objet : {email.subject}\n"
                    f"Corps du message :\n{email.body}"
                )
            ),
        ],
    )
    return await chat_parse_to_model(EmailClassification, request)


@workflows.workflow.define(name="tri_emails")
class TriEmailsWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, input: TriEmailsInput) -> TriEmailsOutput:
        if not input.emails:
            return TriEmailsOutput(emails=[])
        classifications = await workflows.execute_activities_in_parallel(
            activity=classer_email,
            items=input.emails,
            max_concurrent_scheduled_tasks=input.max_concurrent,
        )
        results = classifications or []
        return TriEmailsOutput(
            emails=[
                EmailEnrichi(
                    sender=email.sender,
                    subject=email.subject,
                    **classification.model_dump(),
                )
                for email, classification in zip(input.emails, results)
            ]
        )
