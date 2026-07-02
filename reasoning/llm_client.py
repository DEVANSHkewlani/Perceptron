"""
LLMClient — calls Claude/GPT-4o (paid) or Groq/Ollama (free).
Handles: retry with backoff, JSON extraction, Pydantic validation,
schema-correction re-prompt on parse failure.

The LLM is called ONLY when the fast path has no matching rule.
"""
from __future__ import annotations
import json
import logging
import os
import re
from dataclasses import dataclass

import anthropic
import openai
from pydantic import ValidationError
from tenacity import (
    retry, stop_after_attempt,
    wait_exponential, retry_if_exception_type,
)

from .schema import DecisionObject

log = logging.getLogger("reasoning.llm_client")


@dataclass
class LLMConfig:
    model:       str | None = None
    max_tokens:  int   = 2048
    temperature: float = 0.1     # low temp — we want deterministic decisions
    timeout_s:   float = 30.0
    max_retries: int   = 2


class ParseError(Exception):
    pass


class LLMClient:
    def __init__(self, cfg: LLMConfig | None = None):
        self.cfg = cfg or LLMConfig()
        self._anthropic_client = None
        self._openai_client = None
        self._mode = self._determine_mode()

    def _determine_mode(self) -> str:
        """Determine what client configuration we are using based on environment variables."""
        if os.environ.get("GROQ_API_KEY"):
            log.info("Using GROQ API for reasoning.")
            return "groq"
        elif os.environ.get("USE_OLLAMA") == "true" or (
            not os.environ.get("ANTHROPIC_API_KEY") and not os.environ.get("OPENAI_API_KEY")
        ):
            log.info("No paid API keys found. Defaulting to local Ollama.")
            return "ollama"
        else:
            log.info("Using paid API keys (Anthropic + OpenAI fallback).")
            return "paid"

    def _get_anthropic_client(self) -> anthropic.AsyncAnthropic:
        if self._anthropic_client is None:
            self._anthropic_client = anthropic.AsyncAnthropic(
                api_key=os.environ.get("ANTHROPIC_API_KEY")
            )
        return self._anthropic_client

    def _get_openai_client(self) -> openai.AsyncOpenAI:
        if self._openai_client is None:
            if self._mode == "groq":
                self._openai_client = openai.AsyncOpenAI(
                    base_url="https://api.groq.com/openai/v1",
                    api_key=os.environ.get("GROQ_API_KEY"),
                )
            elif self._mode == "ollama":
                self._openai_client = openai.AsyncOpenAI(
                    base_url=os.environ.get("OLLAMA_HOST", "http://localhost:11434/v1"),
                    api_key="ollama",  # Ollama doesn't validate keys
                )
            else:
                self._openai_client = openai.AsyncOpenAI(
                    api_key=os.environ.get("OPENAI_API_KEY")
                )
        return self._openai_client

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((anthropic.APIStatusError, openai.APIStatusError, ParseError)),
    )
    async def call(self, prompt: str) -> DecisionObject:
        """
        Call the LLM with the assembled prompt.
        Returns a validated DecisionObject.
        Retries up to 3 times on API errors or parse failures.
        """
        # --- GROQ MODE ---
        if self._mode == "groq":
            client = self._get_openai_client()
            model = self.cfg.model
            if not model or "claude" in model.lower() or "gpt" in model.lower():
                model = "llama-3.3-70b-versatile"
            response = await client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.cfg.max_tokens,
                temperature=self.cfg.temperature,
                timeout=self.cfg.timeout_s,
            )
            raw_text = response.choices[0].message.content or ""
            return self._parse_and_validate(raw_text)

        # --- OLLAMA MODE ---
        elif self._mode == "ollama":
            client = self._get_openai_client()
            model = self.cfg.model
            if not model or "claude" in model.lower() or "gpt" in model.lower():
                model = "llama3.2"
            response = await client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.cfg.max_tokens,
                temperature=self.cfg.temperature,
                timeout=self.cfg.timeout_s,
            )
            raw_text = response.choices[0].message.content or ""
            return self._parse_and_validate(raw_text)

        # --- PAID MODE (Anthropic primary, OpenAI fallback) ---
        else:
            try:
                client = self._get_anthropic_client()
                model = self.cfg.model or "claude-3-5-sonnet-20241022"
                response = await client.messages.create(
                    model=model,
                    max_tokens=self.cfg.max_tokens,
                    temperature=self.cfg.temperature,
                    messages=[{"role": "user", "content": prompt}],
                    timeout=self.cfg.timeout_s,
                )
                raw_text = response.content[0].text if response.content else ""
                return self._parse_and_validate(raw_text)
            except Exception as e:
                log.warning(f"Primary Anthropic LLM client failed: {e}. Trying OpenAI fallback...")
                try:
                    client = self._get_openai_client()
                    response = await client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=self.cfg.max_tokens,
                        temperature=self.cfg.temperature,
                        timeout=self.cfg.timeout_s,
                    )
                    raw_text = response.choices[0].message.content or ""
                    return self._parse_and_validate(raw_text)
                except Exception as oai_err:
                    log.error(f"Fallback OpenAI LLM client failed: {oai_err}")
                    raise oai_err

    def _parse_and_validate(self, raw_text: str) -> DecisionObject:
        """
        Extract JSON from LLM output and validate against DecisionObject schema.
        LLMs sometimes wrap JSON in markdown fences — strip those first.
        On Pydantic validation error → raise ParseError to trigger retry.
        """
        # Strip markdown fences if present
        text = raw_text.strip()
        fence_match = re.search(r"`{3}(?:json)?\s*([\s\S]+?)\s*`{3}", text)
        if fence_match:
            text = fence_match.group(1)

        # Find the outermost JSON object
        brace_match = re.search(r"\{[\s\S]+\}", text)
        if not brace_match:
            raise ParseError(f"No JSON object found in LLM output: {text[:200]}")

        try:
            data = json.loads(brace_match.group())
        except json.JSONDecodeError as e:
            raise ParseError(f"JSON decode error: {e}. Raw: {text[:200]}")

        try:
            return DecisionObject(**data)
        except ValidationError as e:
            raise ParseError(f"Schema validation failed: {e}")

    async def call_with_schema_correction(self, prompt: str) -> DecisionObject:
        """
        Two-attempt wrapper: first call, then if parse fails,
        send a correction prompt with the validation error.
        """
        try:
            return await self.call(prompt)
        except ParseError as e:
            correction = (
                prompt + f"\n\nYour previous response had a schema error: {e}\n"
                "Please respond ONLY with valid JSON matching the exact schema above."
            )
            return await self.call(correction)
