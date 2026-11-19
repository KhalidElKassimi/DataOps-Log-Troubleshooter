import ast
import base64
import io
import json
import re
from pathlib import Path
import sys
import types
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request

from databricks.sdk.service.jobs import NotebookTask, Run, RunOutput, RunTask
from databricks.sdk.service.workspace import ExportResponse


ROOT = Path(__file__).resolve().parents[1]


class AgentContextTests(unittest.TestCase):
    def test_notebook_cells_compile_individually(self):
        for path in sorted((ROOT / "notebooks").glob("*.py")):
            source = path.read_text(encoding="utf-8")
            cells = re.split(r"^[ \t]*# COMMAND[^\n]*(?:\n|$)", source, flags=re.MULTILINE)
            for index, cell in enumerate(cells, 1):
                with self.subTest(notebook=path.name, cell=index):
                    compile(cell, f"{path.name}:cell-{index}", "exec")

    def setUp(self):
        path = ROOT / "notebooks" / "02_troubleshooter_agent.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        tree.body = [
            node for node in tree.body
            if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.ClassDef))
        ]
        self.ns = {}
        exec(compile(tree, str(path), "exec"), self.ns)
        self.helper_class = self.ns["DatabricksDevOpsHelper"]
        self.ns.update(
            foundry_api_key="model-secret", ado_pat="ado-secret", outlook_logic_app_url="",
            ado_org="org", ado_proj="project", deployed_commit="abc123",
            failed_task_run_id="123", job_run_id="456", INGEST_SOURCE=None, RUN_CONTEXT={},
            LAST_WORK_ITEM_ID=13, LAST_PR_ID=None,
            INGEST_NOTEBOOK_PATH="/notebooks/01_ingest_sales_pipeline.py",
        )
        self.content = (
            "# Databricks notebook source\n"
            "bad_batch_schema = [('unit_price', 'double', False)]\n"
            "bad_data = [('TXN_1005', 14, None, 3)]\n"
        )
        self.fingerprint = self.ns["source_fingerprint"](self.content)
        self.client = Mock()
        self.client.jobs.get_run_output.return_value = RunOutput(
            error="PySparkValueError: CANNOT_BE_NONE",
            error_trace="unit_price nullable=False\nbad_data = [('TXN_1005', 14, None, 3)]\nToken=hidden\nmodel-secret",
        )
        self.client.jobs.get_run.return_value = Run(
            run_id=456,
            tasks=[RunTask(run_id=789, notebook_task=NotebookTask(notebook_path="/Shared/unrelated")),
                   RunTask(run_id=123, notebook_task=NotebookTask(
                notebook_path=f"/Shared/dataops_versions/ingest_{self.fingerprint}"
            ))]
        )
        self.client.workspace.export.return_value = ExportResponse(
            content=base64.b64encode(self.content.encode()).decode()
        )
        sdk = types.ModuleType("databricks.sdk")
        sdk.WorkspaceClient = Mock(return_value=self.client)
        errors = types.ModuleType("databricks.sdk.errors")
        errors.DatabricksError = type("DatabricksError", (Exception,), {})
        workspace = types.ModuleType("databricks.sdk.service.workspace")
        workspace.ExportFormat = types.SimpleNamespace(SOURCE="SOURCE")
        self.modules = {
            "databricks.sdk": sdk, "databricks.sdk.errors": errors,
            "databricks.sdk.service.workspace": workspace,
        }

    def load(self):
        with patch.dict(sys.modules, self.modules), redirect_stdout(io.StringIO()):
            return self.ns["load_failed_task_error"]("123")

    def test_trace_and_executed_source_are_available_and_secrets_masked(self):
        self.assertIn("CANNOT_BE_NONE", self.load())
        context = self.ns["RUN_CONTEXT"]
        self.assertTrue(context["source_verified"])
        self.assertIn("None", context["trace"])
        self.assertIn("nullable=False", context["trace"])
        self.assertNotIn("hidden", context["trace"])
        self.assertNotIn("model-secret", context["trace"])
        self.assertEqual(context["executed_source"], self.content)
        self.client.jobs.get_run.assert_called_once_with(run_id=456)
        self.client.workspace.export.assert_called_once_with(
            path=f"/Shared/dataops_versions/ingest_{self.fingerprint}", format="SOURCE",
        )

    def test_modified_snapshot_or_legacy_path_is_not_trusted(self):
        for path in ("/Shared/01_ingest_sales_pipeline", "/Shared/dataops_versions/ingest_" + "0" * 64):
            with self.subTest(path=path):
                self.client.jobs.get_run.return_value.tasks[1].notebook_task.notebook_path = path
                self.load()
                self.assertFalse(self.ns["RUN_CONTEXT"]["source_verified"])

    def test_missing_or_unmatched_parent_blocks_source_export_not_diagnosis(self):
        for tasks in (None, [], [RunTask(run_id=789)], [RunTask(run_id=123)]):
            with self.subTest(tasks=tasks):
                self.client.jobs.get_run.return_value = Run(tasks=tasks)
                self.assertIn("CANNOT_BE_NONE", self.load())
                self.assertFalse(self.ns["RUN_CONTEXT"]["source_verified"])
        self.ns["job_run_id"] = ""
        self.assertIn("CANNOT_BE_NONE", self.load())
        self.assertFalse(self.ns["RUN_CONTEXT"]["source_verified"])
        self.client.workspace.export.assert_not_called()

    def test_missing_error_stops_diagnosis(self):
        self.client.jobs.get_run_output.return_value.error = ""
        with self.assertRaises(RuntimeError):
            self.load()
        self.client.workspace.export.assert_not_called()

    def test_different_repository_source_blocks_pr_before_writes(self):
        self.load()
        helper = Mock()
        helper.get_repositories.return_value = [{"id": "repo", "name": "repo"}]
        helper.get_branch_commit.return_value = "main-commit"
        helper.get_file_at_commit.return_value = self.content.replace("None", "42")
        self.ns["DatabricksDevOpsHelper"] = Mock(return_value=helper)
        with redirect_stdout(io.StringIO()):
            result = self.ns["execute_tool"]("read_ingestion_notebook", {})
            self.assertFalse(result["matches_executed_source"])
            result = self.ns["execute_tool"]("create_azure_devops_pull_request", {})
        self.assertEqual(result["status"], "error")
        helper.create_branch_and_commit.assert_not_called()
        helper.create_pull_request.assert_not_called()

    def test_matching_source_allows_only_targeted_draft_pr(self):
        self.load()
        helper = Mock()
        helper.get_repositories.return_value = [{"id": "repo", "name": "repo"}]
        helper.get_branch_commit.return_value = "main-commit"
        helper.get_file_at_commit.return_value = self.content
        helper.create_pull_request.return_value = {"pullRequestId": 15}
        self.ns["DatabricksDevOpsHelper"] = Mock(return_value=helper)
        original = "bad_data = [('TXN_1005', 14, None, 3)]"
        replacement = "bad_data = [('TXN_1005', 14, 12.5, 3)]"
        # This fixture tests patch mechanics, not a business rule or a proposed price.
        with redirect_stdout(io.StringIO()):
            self.ns["execute_tool"]("read_ingestion_notebook", {})
            result = self.ns["execute_tool"]("create_azure_devops_pull_request", {
                "title": "test", "original_section": original,
                "replacement_section": replacement, "explanation": "fixture",
            })
        self.assertEqual(result["status"], "created")
        self.assertEqual(
            helper.create_branch_and_commit.call_args.kwargs["new_content"],
            self.content.replace(original, replacement),
        )
        self.assertIn("non exécuté dans Spark", helper.create_pull_request.call_args.kwargs["description"])
    def test_draft_flag_and_full_rewrite_rejection(self):
        helper = self.helper_class("org", "project", "fake")
        helper.request = Mock()
        helper.create_pull_request("repo", "fix", "main", "title", "description")
        self.assertTrue(helper.request.call_args.kwargs["data"]["isDraft"])
        with self.assertRaises(ValueError):
            self.ns["apply_section_patch"](self.content, self.content, "pass")

    def test_job_uses_content_addressed_source_and_deployment_commit(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location("deploy", ROOT / "scripts" / "deploy_to_databricks.py")
        deploy = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(deploy)
        content = (ROOT / "notebooks" / "01_ingest_sales_pipeline.py").read_text(encoding="utf-8")
        expected = "/Shared/dataops_versions/ingest_" + self.ns["source_fingerprint"](content)
        with (
            patch.dict("os.environ", {"BUILD_SOURCEVERSION": "deployed-sha"}),
            patch("urllib.request.urlopen", side_effect=[
                io.BytesIO(b'{"jobs": []}'), io.BytesIO(b'{"job_id": 1}'),
            ]) as request,
            redirect_stdout(io.StringIO()),
        ):
            deploy.get_or_create_job("https://example.invalid", {})
        settings = json.loads(request.call_args.args[0].data)
        self.assertEqual(settings["tasks"][0]["notebook_task"]["notebook_path"], expected)
        params = settings["tasks"][1]["notebook_task"]["base_parameters"]
        self.assertEqual(params["deployed_commit"], "deployed-sha")
        self.assertEqual(params["failed_task_run_id"], "{{tasks.ingest_sales_pipeline.run_id}}")
        self.assertEqual(params["job_run_id"], "{{job.run_id}}")

    def test_rate_limit_respects_delays_and_reuses_same_request(self):
        req = Request("https://example.invalid", data=b"{}")
        for headers, delay in (({"Retry-After": "9"}, 9), ({"retry-after-ms": "250"}, 0.25), ({}, 5)):
            with self.subTest(headers=headers), patch("urllib.request.urlopen", side_effect=[
                HTTPError(req.full_url, 429, "rate limited", headers, None),
                io.BytesIO(b'{"choices": []}'),
            ]) as send, patch("time.sleep") as sleep, redirect_stdout(io.StringIO()):
                self.ns["request_model"](req)
                self.assertEqual(send.call_count, 2)
                self.assertIs(send.call_args_list[0].args[0], send.call_args_list[1].args[0])
                sleep.assert_called_once_with(delay)

    def test_rate_limit_is_bounded_and_other_http_errors_not_retried(self):
        req = Request("https://example.invalid", data=b"{}")
        for status, headers, calls in ((429, {}, 4), (429, {"Retry-After": "300"}, 1), (401, {}, 1)):
            with self.subTest(status=status, headers=headers), patch("urllib.request.urlopen", side_effect=HTTPError(
                req.full_url, status, "rejected", headers, None,
            )) as send, patch("time.sleep"), redirect_stdout(io.StringIO()):
                with self.assertRaises((RuntimeError, HTTPError)):
                    self.ns["request_model"](req)
                self.assertEqual(send.call_count, calls)

    def test_rate_limit_after_pr_does_not_repeat_tool_and_continues_to_email(self):
        def response(tool):
            message = {"content": "done"}
            if tool:
                message["tool_calls"] = [{
                    "id": tool, "function": {"name": tool, "arguments": "{}"},
                }]
            return io.BytesIO(json.dumps({"choices": [{"message": message}]}).encode())

        self.ns.update(
            foundry_endpoint="https://example.invalid", ai_model_name="test", pipeline_name="test",
            SYSTEM_PROMPT="test", AGENT_TOOLS=[],
        )
        tools = [
            "create_azure_devops_work_item", "read_ingestion_notebook", "assess_incident",
            "create_azure_devops_pull_request", "send_outlook_email",
        ]
        execute = self.ns["execute_tool"] = Mock(side_effect=[
            {"status": "created"}, {"status": "read"}, {"status": "assessed", "category": "code"},
            {"status": "created"}, {"status": "sent"},
        ])
        with patch("urllib.request.urlopen", side_effect=[
            *(response(tool) for tool in tools[:4]),
            HTTPError("https://example.invalid", 429, "rate limited", {}, None),
            response("send_outlook_email"), response(None),
        ]) as send, patch("time.sleep"), redirect_stdout(io.StringIO()):
            self.ns["run_agent_diagnosis"]("error")
        self.assertEqual([call.args[0] for call in execute.call_args_list], tools)
        self.assertEqual(send.call_count, 7)
        self.assertEqual(send.call_args_list[4].args[0].data, send.call_args_list[5].args[0].data)

    def test_unchanged_cell_markers_in_context_do_not_block_targeted_patch(self):
        source = "# Databricks notebook source\r\nx = 0\r\n# COMMAND ----------\r\ny = 1\r\n# COMMAND ----------\r\nz = 2\r\n"
        original = "# COMMAND ----------\ny = 1\n# COMMAND ----------"
        self.assertEqual(
            self.ns["apply_section_patch"](source, original, original.replace("y = 1", "y = 3")),
            source.replace("y = 1", "y = 3"),
        )
        with self.assertRaises(ValueError):
            self.ns["apply_section_patch"](source, original, "y = 3")

    def test_code_assessment_requires_evidence_from_read_source(self):
        self.ns["INGEST_SOURCE"] = {"content": self.content}
        with redirect_stdout(io.StringIO()):
            result = self.ns["execute_tool"]("assess_incident", {
                "category": "code", "reason": "Non nullable field receives None.",
                "evidence": "unknown_code()",
            })
            self.assertEqual(result["status"], "error")
            result = self.ns["execute_tool"]("assess_incident", {
                "category": "code", "reason": "Non nullable field receives None.",
                "evidence": "bad_data = [('TXN_1005', 14, None, 3)]",
            })
            self.assertEqual(result["status"], "assessed")
            self.assertEqual(result["category"], "code")

    def test_code_fix_is_required_and_invalid_patches_are_reformulated(self):
        self._check_pr_workflow("code", [
            {"status": "error", "retryable": True, "message": "patch invalide"},
            {"status": "created"},
        ])

    def test_non_code_error_does_not_force_pr(self):
        self._check_pr_workflow("configuration", [])

    def test_repeated_patch_failures_send_alert_but_fail_job_explicitly(self):
        with self.assertRaisesRegex(RuntimeError, "PR non créée"):
            self._check_pr_workflow("code", [
                {"status": "error", "retryable": True, "message": "patch invalide"},
            ] * 3)

    def _check_pr_workflow(self, category, patches):
        self.ns.update(
            foundry_endpoint="https://example.invalid", ai_model_name="test", pipeline_name="test",
            SYSTEM_PROMPT="test", AGENT_TOOLS=[],
        )
        tools = [
            "create_azure_devops_work_item", "read_ingestion_notebook", "assess_incident",
            *(["create_azure_devops_pull_request"] * len(patches)), "send_outlook_email",
        ]
        replies = [{
            "choices": [{"message": {"tool_calls": [
                {"id": str(index), "function": {"name": name, "arguments": "{}"}},
            ]}}],
        } for index, name in enumerate(tools)]
        replies.append({"choices": [{"message": {"content": "done"}}]})
        execute = self.ns["execute_tool"] = Mock(side_effect=[
            {"status": "created"}, {"status": "read"}, {"status": "assessed", "category": category},
            *patches, {"status": "sent"},
        ])
        model = self.ns["request_model"] = Mock(side_effect=replies)
        try:
            with redirect_stdout(io.StringIO()):
                self.ns["run_agent_diagnosis"]("error")
        finally:
            self.assertEqual([call.args[0] for call in execute.call_args_list], tools)
            for call, name in zip(model.call_args_list, tools):
                payload = json.loads(call.args[0].data)
                self.assertEqual(payload["tool_choice"]["function"]["name"], name)
                self.assertFalse(payload["parallel_tool_calls"])

if __name__ == "__main__":
    unittest.main()
