"""Guards for learner-friendly public API documentation."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCUMENTED_ROOTS = (
    PROJECT_ROOT / "src" / "order_app",
    PROJECT_ROOT / "tests" / "helpers",
)


def _public_callables() -> list[tuple[Path, ast.FunctionDef | ast.AsyncFunctionDef]]:
    callables: list[tuple[Path, ast.FunctionDef | ast.AsyncFunctionDef]] = []
    for root in DOCUMENTED_ROOTS:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if not node.name.startswith("_"):
                        callables.append((path, node))
                    continue
                if isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
                    for method in node.body:
                        if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            if not method.name.startswith("_"):
                                callables.append((path, method))
    return callables


def _public_classes() -> list[tuple[Path, ast.ClassDef]]:
    classes: list[tuple[Path, ast.ClassDef]] = []
    for root in DOCUMENTED_ROOTS:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            classes.extend(
                (path, node)
                for node in tree.body
                if isinstance(node, ast.ClassDef) and not node.name.startswith("_")
            )
    return classes


@pytest.mark.unit
def test_public_callables_explain_inputs_and_returns() -> None:
    """Require public functions/methods to document behavior, inputs, and output."""
    problems: list[str] = []
    for path, callable_node in _public_callables():
        docstring = ast.get_docstring(callable_node) or ""
        parameters = [
            argument.arg
            for argument in (*callable_node.args.args, *callable_node.args.kwonlyargs)
            if argument.arg not in {"self", "cls"}
        ]
        relative_path = path.relative_to(PROJECT_ROOT)
        location = f"{relative_path}:{callable_node.lineno} {callable_node.name}"
        if not docstring:
            problems.append(f"{location} has no docstring")
            continue
        if parameters and "Args:" not in docstring and "Accepts:" not in docstring:
            problems.append(f"{location} does not explain accepted parameters")
        if "Returns:" not in docstring:
            problems.append(f"{location} does not explain its return value")

    assert problems == [], "\n" + "\n".join(problems)


@pytest.mark.unit
def test_public_classes_explain_their_responsibility() -> None:
    """Require every public class to state why it exists."""
    missing = [
        f"{path.relative_to(PROJECT_ROOT)}:{class_node.lineno} {class_node.name}"
        for path, class_node in _public_classes()
        if not ast.get_docstring(class_node)
    ]

    assert missing == [], "Public classes without docstrings:\n" + "\n".join(missing)
