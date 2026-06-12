# ╔══════════════════════════════════════════════════════╗
#   GROQ AI INTEGRATION — Firebase Structure Learning
#   Replaces old api.g0i.ai endpoint
# ╚══════════════════════════════════════════════════════╝

import asyncio
import json
import re
import logging
from typing import Optional, Tuple

import aiohttp

import config

log = logging.getLogger(__name__)

# ══════════════ GROQ SYSTEM PROMPT ═════════════════════

GROQ_SYSTEM_PROMPT = """You are an expert Firebase Realtime Database analyst specializing in SMS relay systems.

Your task: Given a sample Firebase database structure, identify:
1. Root path containing device IDs
2. Where phone numbers are stored (exact field names)
3. Path template for SMS messages
4. Exact field names for: SMS body, sender, timestamp
5. Device status field and path

Return ONLY valid JSON (no markdown, no explanation, no <think> tags):
{
  "pattern_id": "AI-1",
  "devices_root": "path to devices e.g., 'clients' or 'All_Users/DeviceInfo'",
  "phone_field": "field name holding phone number, or null if not found",
  "phone_path_template": "e.g., 'clients/{device_id}' or null",
  "sms_root": "path template e.g., 'messages/{device_id}' or 'user_sms/{device_id}'",
  "sms_body_field": "e.g., 'message', 'body', 'msg'",
  "sms_sender_field": "e.g., 'sender', 'from', 'address'",
  "sms_time_field": "e.g., 'timestamp', 'dateTime', 'receivedDate'",
  "status_root": "e.g., 'clients/{device_id}' or null",
  "status_field": "e.g., 'status', 'online', 'isOnline'",
  "confidence": 0.95,
  "notes": "brief analysis explanation"
}"""


async def call_groq(prompt: str, timeout: int = None) -> Optional[str]:
    """
    Call Groq API (llama-3.3-70b-versatile or deepseek-r1).
    Returns raw response content or None on error.
    """
    if not config.GROQ_API_KEY:
        log.error("GROQ_API_KEY not configured")
        return None

    timeout = timeout or config.GROQ_TIMEOUT
    headers = {
        "Authorization": f"Bearer {config.GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": config.GROQ_MODEL,
        "messages": [
            {"role": "system", "content": GROQ_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 800,
    }

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(
                f"{config.GROQ_BASE_URL}/chat/completions",
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    err_text = await resp.text()
                    log.error(f"Groq API error {resp.status}: {err_text[:200]}")
                    return None

                data = await resp.json(content_type=None)
                return data["choices"][0]["message"]["content"].strip()

    except asyncio.TimeoutError:
        log.error(f"Groq request timeout after {timeout}s")
        return None
    except Exception as e:
        log.error(f"Groq request failed: {e}")
        return None


def parse_groq_response(raw_response: str) -> Optional[dict]:
    """
    Parse Groq response and extract JSON.
    Handles markdown code blocks and strips thinking tags.
    """
    if not raw_response:
        return None

    try:
        # Strip markdown code blocks
        content = re.sub(r"^```[a-z]*\n?", "", raw_response)
        content = re.sub(r"\n?```$", "", content)

        # Strip Groq thinking tags
        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

        # Try to extract JSON object
        pattern = re.search(r"\{.*\}", content, re.DOTALL)
        if pattern:
            return json.loads(pattern.group(0))

        return None
    except Exception as e:
        log.error(f"Failed to parse Groq response: {e}")
        return None


async def ai_learn_firebase_structure(
    base_url: str,
    sample_data: dict,
    api_key: str = None,
    timeout: int = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """
    Use Groq to learn an unknown Firebase structure.
    
    Args:
        base_url: Firebase base URL
        sample_data: Sample data from Firebase (shallow sample)
        api_key: Optional override API key
        timeout: Optional override timeout
    
    Returns:
        (pattern_dict, error_message_or_None)
    """
    prompt = f"""Analyze this Firebase database sample and provide the structure mapping:

{json.dumps(sample_data, indent=2, default=str)[:5000]}

Return ONLY the JSON structure mapping, no other text."""

    raw_response = await call_groq(prompt, timeout)
    if not raw_response:
        return None, "Groq API request failed"

    pattern = parse_groq_response(raw_response)
    if not pattern:
        return None, f"Could not parse Groq response: {raw_response[:100]}"

    # Validate pattern has minimum required fields
    required = ["devices_root", "sms_root", "sms_body_field"]
    if not all(pattern.get(k) for k in required):
        return None, f"Pattern incomplete: missing {[k for k in required if not pattern.get(k)]}"

    log.info(f"[Groq] Learned pattern: {pattern.get('pattern_id')} confidence={pattern.get('confidence')}")
    return pattern, None


async def analyze_ghost_device(
    device_id: str,
    raw_firebase_data: dict,
    timeout: int = None,
) -> Optional[dict]:
    """
    Use Groq to analyze a ghost device and suggest recovery paths.
    
    Returns dict with:
    - possible_paths: [list of paths to check]
    - likely_active: bool
    - likely_dead: bool
    - confidence: float (0-1)
    - recommended_action: str
    """
    prompt = f"""Given this Firebase device data for ghost device '{device_id}', 
suggest the most likely path to find its phone number:

{json.dumps(raw_firebase_data, indent=2, default=str)[:3000]}

Return JSON:
{{
  "possible_paths": ["path1", "path2", "path3"],
  "likely_active": true/false,
  "likely_dead": true/false,
  "confidence": 0.85,
  "recommended_action": "string"
}}"""

    raw_response = await call_groq(prompt, timeout)
    if not raw_response:
        return None

    analysis = parse_groq_response(raw_response)
    return analysis if analysis else None


# ══════════════ FIREBASE TESTING ═══════════════════════

async def test_groq_connection() -> Tuple[bool, str]:
    """
    Test Groq API connectivity and basic response.
    Returns (success, message)
    """
    if not config.GROQ_API_KEY:
        return False, "GROQ_API_KEY not configured"

    test_prompt = "Say 'OK' if you understand."
    response = await call_groq(test_prompt, timeout=10)

    if response and ("ok" in response.lower() or "yes" in response.lower()):
        return True, "Groq API is working"
    elif response:
        return True, f"Groq response: {response[:50]}"
    else:
        return False, "Groq API not responding"
