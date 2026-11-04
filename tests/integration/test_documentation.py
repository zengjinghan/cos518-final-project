import re
from pathlib import Path

import pytest


def test_readme_python_walkthrough_is_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    readme = Path(__file__).resolve().parents[2] / "README.md"
    blocks = re.findall(r"```python\n(.*?)```", readme.read_text(encoding="utf-8"), re.DOTALL)
    assert blocks
    monkeypatch.chdir(tmp_path)
    for index, block in enumerate(blocks):
        namespace: dict[str, object] = {}
        code = compile(block, f"README.md:python-example-{index}", "exec")
        exec(code, namespace)  # noqa: S102 - Execute owned walkthroughs in an isolated temp directory.


def test_documentation_links_resolve_within_the_project() -> None:
    root = Path(__file__).resolve().parents[2]
    documents = (root / "README.md", *sorted((root / "docs").glob("*.md")))
    for document in documents:
        text = document.read_text(encoding="utf-8")
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
            if target.startswith(("https://", "http://", "#")):
                continue
            path = (document.parent / target.partition("#")[0]).resolve()
            assert path.is_relative_to(root), (document, target)
            assert path.is_file(), (document, target)
