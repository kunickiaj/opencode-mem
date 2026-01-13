from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional


@dataclass
class TypedMemory:
    category: str
    title: str
    body: str
    confidence: float = 0.5
    metadata: Optional[dict[str, Any]] = None


DEFAULT_OPENAI_MODEL = os.getenv("OPENCODE_MEM_OBSERVATION_MODEL", "gpt-5.1-codex-mini")
DEFAULT_ANTHROPIC_MODEL = "claude-4.5-haiku"


class ObservationClassifier:
    def __init__(self) -> None:
        provider = os.getenv("OPENCODE_MEM_OBSERVATION_PROVIDER", "openai").lower()
        self.provider = "anthropic" if provider == "anthropic" else "openai"
        self.model = (
            DEFAULT_ANTHROPIC_MODEL
            if self.provider == "anthropic"
            else DEFAULT_OPENAI_MODEL
        )
        self.client: Any = None
        self.api_key = os.getenv("OPENCODE_MEM_OBSERVATION_API_KEY")
        if self.provider == "anthropic":
            if not self.api_key:
                self.client = None
                return
            try:
                import anthropic  # type: ignore

                self.client = anthropic.Anthropic(api_key=self.api_key)
            except Exception:  # pragma: no cover
                self.client = None
        else:
            if not self.api_key:
                self.client = None
                return
            try:
                from openai import OpenAI  # type: ignore

                self.client = OpenAI(api_key=self.api_key)
            except Exception:  # pragma: no cover
                self.client = None

    def available(self) -> bool:
        return self.client is not None

    def classify(
        self,
        transcript: str,
        summary: Any,
        events: Iterable[dict[str, Any]] | None = None,
    ) -> List[TypedMemory]:
        if not self.available():
            return self._heuristic_classify(summary)
        payload = self._build_payload(transcript, summary, events)
        text = self._call_model(payload)
        if not text:
            return self._heuristic_classify(summary)
        memories = self._parse_response(text)
        if not memories:
            return self._heuristic_classify(summary)
        return memories

    def _build_payload(
        self, transcript: str, summary: Any, events: Iterable[dict[str, Any]] | None
    ) -> str:
        obs = summary.observations if hasattr(summary, "observations") else []
        obs_text = "\n".join(obs[:5])
        context = events or []
        context_text = json.dumps(context, ensure_ascii=False)[:2000]
        prompt_parts = [
            "You are a ferociously accurate memory curator.",
            "Categorize the session into memory types: prompt, discovery, change, decision.",
            "Return JSON array of objects with category,title,body,confidence.",
            "Session summary:",
            summary.session_summary if hasattr(summary, "session_summary") else "",
            "Key observations:",
            obs_text,
            "Recent tool events:",
            context_text,
        ]
        return "\n".join(part for part in prompt_parts if part)

    def _call_model(self, prompt: str) -> Optional[str]:
        try:
            if self.provider == "anthropic" and self.client:
                resp = self.client.completions.create(
                    model=self.model,
                    prompt=f"\nHuman: {prompt}\nAssistant:",
                    temperature=0,
                    max_tokens=400,
                )
                return resp.completion
            if self.client:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You categorize memories."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0,
                    max_tokens=400,
                )
                return resp.choices[0].message.content
        except Exception:  # pragma: no cover
            return None
        return None

    def _parse_response(self, text: str) -> List[TypedMemory]:
        try:
            data = json.loads(text)
        except Exception:  # pragma: no cover
            return []
        if not isinstance(data, list):
            return []
        results: List[TypedMemory] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            category = item.get("category") or item.get("type")
            if not category or category not in {
                "prompt",
                "discovery",
                "change",
                "decision",
            }:
                continue
            title = (
                item.get("title") or item.get("body", "").strip().splitlines()[0][:80]
            )
            body = item.get("body") or ""
            confidence = float(item.get("confidence", 0.5))
            metadata = item.get("metadata")
            results.append(
                TypedMemory(
                    category=category,
                    title=title,
                    body=body,
                    confidence=confidence,
                    metadata=metadata,
                )
            )
        return results

    def _heuristic_classify(self, summary: Any) -> List[TypedMemory]:
        observations = summary.observations if hasattr(summary, "observations") else []
        results: List[TypedMemory] = []
        for obs in observations[:6]:
            category = self._detect_category(obs)
            title = obs[:80]
            results.append(
                TypedMemory(category=category, title=title, body=obs, confidence=0.4)
            )
        return results

    def _detect_category(self, text: str) -> str:
        lower = text.lower()
        if any(keyword in lower for keyword in ["decid", "choose", "option", "plan"]):
            return "decision"
        if any(
            keyword in lower for keyword in ["change", "update", "migrat", "refactor"]
        ):
            return "change"
        if any(
            keyword in lower for keyword in ["discover", "learn", "found", "noticed"]
        ):
            return "discovery"
        if any(keyword in lower for keyword in ["prompt", "ask", "question", "need"]):
            return "prompt"
        return "discovery"
