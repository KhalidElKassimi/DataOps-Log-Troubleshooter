import base64
import json
import urllib.request
import urllib.error
import urllib.parse
from typing import Dict, Any, List, Optional

class AzureDevOpsClient:
    def __init__(self, org: str, project: str, pat: str):
        self.org = org
        self.project = project
        self.pat = pat
        # HTTP Basic Auth avec un username vide et le PAT comme password
        auth_str = f":{pat}"
        self.auth_header = f"Basic {base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')}"
        self.base_url = f"https://dev.azure.com/{org}/{project}"

    def _request(self, url: str, method: str = "GET", data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        all_headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        if headers:
            all_headers.update(headers)

        body = json.dumps(data).encode("utf-8") if data is not None else None
        req = urllib.request.Request(url, data=body, headers=all_headers, method=method)

        try:
            with urllib.request.urlopen(req) as resp:
                status = resp.status
                content = resp.read().decode("utf-8")
                if content:
                    return json.loads(content)
                return {"status": status}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            raise Exception(f"HTTP {e.code} Error on {url}: {err_body}")

    def get_project_details(self) -> Dict[str, Any]:
        url = f"https://dev.azure.com/{self.org}/_apis/projects/{self.project}?api-version=7.1"
        return self._request(url)

    def get_teams(self) -> List[Dict[str, Any]]:
        url = f"https://dev.azure.com/{self.org}/_apis/projects/{self.project}/teams?api-version=7.1"
        res = self._request(url)
        return res.get("value", [])

    def get_boards(self, team_name: str) -> List[Dict[str, Any]]:
        encoded_team = urllib.parse.quote(team_name)
        url = f"https://dev.azure.com/{self.org}/{self.project}/{encoded_team}/_apis/work/boards?api-version=7.1-preview.1"
        res = self._request(url)
        return res.get("value", [])

    def get_board_columns(self, team_name: str, board_id: str = "Stories") -> List[Dict[str, Any]]:
        encoded_team = urllib.parse.quote(team_name)
        encoded_board = urllib.parse.quote(board_id)
        url = f"https://dev.azure.com/{self.org}/{self.project}/{encoded_team}/_apis/work/boards/{encoded_board}/columns?api-version=7.1-preview.1"
        res = self._request(url)
        return res.get("value", [])

    def set_board_columns(self, team_name: str, columns: List[Dict[str, Any]], board_id: str = "Stories") -> Dict[str, Any]:
        encoded_team = urllib.parse.quote(team_name)
        encoded_board = urllib.parse.quote(board_id)
        url = f"https://dev.azure.com/{self.org}/{self.project}/{encoded_team}/_apis/work/boards/{encoded_board}/columns?api-version=7.1-preview.1"
        return self._request(url, method="PUT", data=columns)

    def get_repositories(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/_apis/git/repositories?api-version=7.1"
        res = self._request(url)
        return res.get("value", [])

    def create_work_item(self, item_type: str, title: str, description: str, repro_steps: str, board_column: str = "Backlog", priority: int = 1) -> Dict[str, Any]:
        last_err = None
        for it in [item_type, "Issue", "Bug"]:
            try:
                url = f"{self.base_url}/_apis/wit/workitems/${it}?api-version=7.1-preview.3"
                patch_doc = [
                    {"op": "add", "path": "/fields/System.Title", "value": title},
                    {"op": "add", "path": "/fields/System.Description", "value": f"{description}<br/><br/><strong>Trace :</strong><pre>{repro_steps}</pre>"}
                ]
                headers = {"Content-Type": "application/json-patch+json"}
                return self._request(url, method="POST", data=patch_doc, headers=headers)
            except Exception as e:
                last_err = e
                continue
        raise last_err or Exception("Impossible de créer le Work Item.")

    def update_work_item_column(self, work_item_id: int, new_column: str) -> Dict[str, Any]:
        url = f"https://dev.azure.com/{self.org}/_apis/wit/workitems/{work_item_id}?api-version=7.1-preview.3"
        patch_doc = [
            {"op": "add", "path": "/fields/System.BoardColumn", "value": new_column}
        ]
        headers = {"Content-Type": "application/json-patch+json"}
        return self._request(url, method="PATCH", data=patch_doc, headers=headers)

    def get_default_branch_commit(self, repo_id: str, branch: str = "main") -> Optional[str]:
        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/refs?filter=heads/{branch}&api-version=7.1"
        res = self._request(url)
        items = res.get("value", [])
        if items:
            return items[0].get("objectId")
        return None

    def push_initial_commit(self, repo_id: str, files: Dict[str, str], commit_message: str = "Initial commit") -> Dict[str, Any]:
        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/pushes?api-version=7.1"
        changes = []
        for path, content in files.items():
            changes.append({
                "changeType": "add",
                "item": {"path": path},
                "newContent": {
                    "content": content,
                    "contentType": "rawtext"
                }
            })
        payload = {
            "refUpdates": [
                {
                    "name": "refs/heads/main",
                    "oldObjectId": "0000000000000000000000000000000000000000"
                }
            ],
            "commits": [
                {
                    "comment": commit_message,
                    "changes": changes
                }
            ]
        }
        return self._request(url, method="POST", data=payload)

    def push_to_existing_branch(self, repo_id: str, branch: str, files: Dict[str, str], commit_message: str) -> Dict[str, Any]:
        base_commit = self.get_default_branch_commit(repo_id, branch)
        if not base_commit:
            return self.push_initial_commit(repo_id, files, commit_message)

        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/pushes?api-version=7.1"
        changes = []
        for path, content in files.items():
            changes.append({
                "changeType": "add",
                "item": {"path": path},
                "newContent": {
                    "content": content,
                    "contentType": "rawtext"
                }
            })
        payload = {
            "refUpdates": [
                {
                    "name": f"refs/heads/{branch}",
                    "oldObjectId": base_commit
                }
            ],
            "commits": [
                {
                    "comment": commit_message,
                    "changes": changes
                }
            ]
        }
        try:
            return self._request(url, method="POST", data=payload)
        except Exception:
            # Si le fichier existe déjà, tenter en 'edit'
            for c in changes:
                c["changeType"] = "edit"
            return self._request(url, method="POST", data=payload)

    def create_branch_with_fix(self, repo_id: str, base_branch: str, fix_branch: str, file_path: str, new_content: str, commit_message: str) -> Dict[str, Any]:
        base_commit = self.get_default_branch_commit(repo_id, base_branch)
        if not base_commit:
            raise Exception(f"Branche de base '{base_branch}' introuvable.")

        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/pushes?api-version=7.1"
        payload = {
            "refUpdates": [
                {
                    "name": f"refs/heads/{fix_branch}",
                    "oldObjectId": "0000000000000000000000000000000000000000"
                }
            ],
            "commits": [
                {
                    "comment": commit_message,
                    "changes": [
                        {
                            "changeType": "edit",
                            "item": {"path": file_path},
                            "newContent": {
                                "content": new_content,
                                "contentType": "rawtext"
                            }
                        }
                    ]
                }
            ]
        }
        return self._request(url, method="POST", data=payload)

    def create_pull_request(self, repo_id: str, source_branch: str, target_branch: str, title: str, description: str, work_item_ids: Optional[List[int]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/pullrequests?api-version=7.1"
        payload = {
            "sourceRefName": f"refs/heads/{source_branch}",
            "targetRefName": f"refs/heads/{target_branch}",
            "title": title,
            "description": description
        }
        if work_item_ids:
            payload["workItemRefs"] = [{"id": str(wid)} for wid in work_item_ids]
        return self._request(url, method="POST", data=payload)

    def create_pipeline(self, repo_id: str, repo_name: str, yaml_path: str = "/azure-pipelines.yml", pipeline_name: str = "CI-CD-Databricks-Pipeline") -> Dict[str, Any]:
        url = f"{self.base_url}/_apis/pipelines?api-version=7.1"
        payload = {
            "name": pipeline_name,
            "configuration": {
                "type": "yaml",
                "path": yaml_path,
                "repository": {
                    "id": repo_id,
                    "name": repo_name,
                    "type": "azureReposGit"
                }
            }
        }
        return self._request(url, method="POST", data=payload)
