---
name: idea-mcp
description: |
  IntelliJ IDEA MCP for Java code analysis, testing, refactoring, and project navigation. Use when: (1) Checking Java compilation errors (get_file_problems / lint_files), (2) Call hierarchy analysis (analyze_calls), (3) Rename refactoring across the codebase, (4) Searching code or finding files in the project, (5) Running tests or run configurations. Requires IDEA to be running with MCP plugin connected.
---

# IntelliJ IDEA MCP Skill

Only documents what the tools' own descriptions don't say clearly, plus pitfalls found in real use.

## File Reading

Use `read_file` only for what the system `read` tool cannot reach: dependency sources and decompiled classes — get a path from `search_symbol({include_external: true})` and pass it back as is (e.g. `../.m2/.../foo-sources.jar!/pkg/Foo.java`). Hand-written `jrt://` URLs fail. For regular project files prefer the system `read` tool (exact line windows); images, PDFs, and directories also require it.

## Multiple Projects

With several projects open in IDEA, a call can route to a project other than the current directory. Before write operations (`rename_refactoring`, `reformat_file`, `optimize_imports`, `build_project`), pass `projectPath` explicitly — the absolute path of the current working directory. It is undocumented and absent from tool schemas, but direct calls forward it. Never operate on a project outside the current directory.

## Pitfalls

- `read_file`: `limit` is unreliable (server-side, verified with a raw JSON-RPC call) — the result may span more lines than requested, and may contain literal `…N lines truncated…` lines that hide middle content. Use the system `read` tool when you need an exact line window.
- `read_file` structured modes (`indentation` etc., described in JetBrains docs) are NOT implemented in the current build — the `mode` parameter is silently ignored. Read methods via `search_symbol` coordinates + `offset`.
- `execute_tool` silently drops unknown or misspelled parameters; it cannot pass `projectPath`.
- `analyze_calls` rejects a plain class name; it wants a callable FQN (method/constructor). Find one via `search_symbol` first.

## Notes

- `build_project` with `filesToRebuild` compiles specific files much faster than `mvn compile`.
- `get_file_problems` / `lint_files` use IDE inspections and are more accurate than command-line builds for multi-module Maven.
- `rename_refactoring`, `reformat_file`, `optimize_imports` write files without confirmation; use `rename_refactoring({preview: true})` to inspect the impact first.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Verify IDEA running with MCP plugin |
| Request timed out | Accept permission dialog in IDEA |
| File not found | Paths are project-relative; verify the project routing (see Multiple Projects) |
