import json
import urllib.request
from typing import Dict, Any, List

from .tools import AGENT_TOOLS, send_teams_alert, create_azure_devops_work_item, create_azure_devops_pull_request
from .prompts import DATAOPS_AGENT_SYSTEM_PROMPT

class DataOpsAgent:
    def __init__(self, foundry_endpoint: str, api_key: str, model_name: str = "gpt-4.1-mini"):
        self.endpoint = foundry_endpoint.rstrip("/")
        self.api_key = api_key
        self.model_name = model_name

    def execute_tools(self, tool_calls: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Exécute les fonctions demandées par l'agent Foundry."""
        results = []
        for call in tool_calls:
            func = call.get("function", {})
            name = func.get("name")
            args = json.loads(func.get("arguments", "{}"))

            print(f"\n⚡ [Tool Calling] L'agent déclenche l'action : '{name}'")
            if name == "notify_teams":
                output = send_teams_alert(args)
            elif name == "create_azure_devops_work_item":
                output = create_azure_devops_work_item(args)
            elif name == "create_azure_devops_pull_request":
                output = create_azure_devops_pull_request(args)
            else:
                output = {"status": "error", "message": f"Outil inconnu {name}"}

            results.append({
                "tool_call_id": call.get("id"),
                "output": json.dumps(output)
            })
        return results

    def troubleshoot_and_act(self, raw_error_log: str, pipeline_name: str) -> Dict[str, Any]:
        """Interroge Microsoft Azure AI Foundry avec support complet des Tools."""
        url = f"{self.endpoint}/openai/deployments/{self.model_name}/chat/completions?api-version=2024-08-01-preview"

        payload = {
            "messages": [
                {"role": "system", "content": DATAOPS_AGENT_SYSTEM_PROMPT},
                {"role": "user", "content": f"Pipeline en échec : {pipeline_name}\n\nVoici le log d'erreur technique brut :\n{raw_error_log}"}
            ],
            "tools": AGENT_TOOLS,
            "tool_choice": "auto",
            "temperature": 0.1
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "api-key": self.api_key
            },
            method="POST"
        )

        with urllib.request.urlopen(req) as response:
            res_data = json.loads(response.read().decode("utf-8"))
            choice = res_data["choices"][0]["message"]
            
            tool_calls = choice.get("tool_calls", [])
            if tool_calls:
                self.execute_tools(tool_calls)
            return {
                "reasoning": choice.get("content"),
                "tool_calls_count": len(tool_calls),
                "status": "success"
            }
