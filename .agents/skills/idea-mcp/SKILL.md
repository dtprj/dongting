---
name: idea-mcp
description: |
  IntelliJ IDEA MCP for Java code analysis, testing, refactoring, and project navigation. Use when: (1) Checking Java compilation errors (get_file_problems is more accurate than LSP for multi-module Maven), (2) Performing rename refactoring across the codebase, (3) Searching code or finding files in the project. Requires IDEA to be running with MCP plugin connected.
---

# IntelliJ IDEA MCP Skill

## Path Rules (Most Common Error Source)

| Parameter | Path Type | Example |
|-----------|-----------|---------|
| `projectPath` | **ABSOLUTE** | `/Users/huangli/dt/dongting` |
| `filePath`, `pathInProject`, `directoryPath`, `contextPath` | **RELATIVE** | `server/src/main/java/...` |

Getting this wrong is the #1 cause of "file not found" errors.

## File Reading

For reading Java source files, prefer IDEA's **`read_file`** over the system's `read` tool. It supports structured modes like `indentation` (reads by code structure/levels) and precise range reads.

## Gotchas and Tips

### Two Search API Families

IDEA MCP has two sets of search tools that return different formats. Pick the right one:

- **`search_in_files_by_text` / `search_in_files_by_regex`** — Returns matched lines with `||highlight||` markers. Good for quick "where is this string used" lookups. Supports `fileMask` filter.
- **`search_text` / `search_regex` / `search_file` / `search_symbol`** — Returns structured results with exact coordinates (line, column, byte offsets). Supports `paths` glob filters and `!` excludes. Use this when you need precise locations for further operations.
  - `search_symbol` is especially powerful — semantic symbol lookup (classes, methods, fields) returning full code snippets.

### `execute_terminal_command` Does Not Use Shell by Default

Without `executeInShell: true`, shell features like `$()` expansion, pipes, and wildcards won't work. For most shell commands, prefer the Bash tool directly.

### `rename_refactoring` Takes Symbol Name, Not Position

Parameters are `symbolName` (the exact identifier string) and `newName`, not file path + line/column. This is different from LSP-based rename.

### `build_project` for Single-File Compilation

`build_project` with `filesToRebuild` parameter compiles specific files much faster than running `mvn compile`. Useful for quick verification after edits.

### `get_file_problems` vs LSP Diagnostics

For multi-module Maven projects, `get_file_problems` is significantly more accurate than LSP `lsp_diagnostics`. Prefer it for Java error checking.

## High-Risk Operations

- `rename_refactoring` & `replace_text_in_file` — Modify code globally, verify with `get_file_problems` after use
- `reformat_file` — Modifies formatting without confirmation
- The High-Risk Operations may be disabled and invisible.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Verify IDEA running with MCP plugin |
| Request timed out | Accept permission dialog in IDEA |
| File not found | Check absolute vs relative path (see Path Rules above) |
