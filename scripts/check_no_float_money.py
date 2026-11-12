"""Static check: no float anywhere near money.

docs/ARCHITECTURE.md §5.2 verification. This walks the AST of app/ and fails
the build if a monetary identifier is annotated as float, or if float()
is called on one. Cheap to run, and it catches the one mistake that
cannot be fixed later.
"""

from __future__ import annotations

import ast
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
APP = ROOT / "app"

MONEY_WORDS = ("amount", "balance", "money", "minor", "total", "fee", "price")


def _is_monetary(name: str) -> bool:
    lowered = name.lower()
    return any(word in lowered for word in MONEY_WORDS)


def _annotation_mentions_float(node: ast.AST | None) -> bool:
    if node is None:
        return False
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name) and sub.id == "float":
            return True
        if isinstance(sub, ast.Constant) and sub.value == "float":
            return True
    return False


def check_file(path: pathlib.Path) -> list[str]:
    problems: list[str] = []
    tree = ast.parse(path.read_text(), filename=str(path))

    for node in ast.walk(tree):
        # x: float = ...  /  self.amount: float
        if isinstance(node, ast.AnnAssign):
            target = node.target
            name = (
                target.id
                if isinstance(target, ast.Name)
                else getattr(target, "attr", "")
            )
            if _is_monetary(name) and _annotation_mentions_float(node.annotation):
                problems.append(
                    f"{path.relative_to(ROOT)}:{node.lineno}: "
                    f"monetary name '{name}' annotated as float"
                )

        # def f(amount: float)  and  -> float
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = [*node.args.args, *node.args.kwonlyargs, *node.args.posonlyargs]
            for arg in args:
                if _is_monetary(arg.arg) and _annotation_mentions_float(
                    arg.annotation
                ):
                    problems.append(
                        f"{path.relative_to(ROOT)}:{arg.lineno}: "
                        f"parameter '{arg.arg}' annotated as float"
                    )
            if _is_monetary(node.name) and _annotation_mentions_float(node.returns):
                problems.append(
                    f"{path.relative_to(ROOT)}:{node.lineno}: "
                    f"function '{node.name}' returns float"
                )

        # float(amount)
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "float" and node.args:
                arg = node.args[0]
                name = (
                    arg.id
                    if isinstance(arg, ast.Name)
                    else getattr(arg, "attr", "")
                )
                if _is_monetary(str(name)):
                    problems.append(
                        f"{path.relative_to(ROOT)}:{node.lineno}: "
                        f"float() called on monetary value '{name}'"
                    )
    return problems


def main() -> int:
    all_problems: list[str] = []
    files = sorted(APP.rglob("*.py"))
    for path in files:
        all_problems.extend(check_file(path))

    if all_problems:
        print("FAIL: float found in monetary code")
        for problem in all_problems:
            print("  " + problem)
        return 1

    print(f"PASS: no float in monetary code ({len(files)} files checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
