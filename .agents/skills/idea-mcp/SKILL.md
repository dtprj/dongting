---
name: idea-mcp
description: |
  IntelliJ IDEA MCP for Java code analysis, testing, refactoring, and project navigation. Use when: (1) Checking Java compilation errors (get_file_problems is more accurate than LSP for multi-module Maven), (2) Performing rename refactoring across the codebase, (3) Searching code or finding files in the project. Requires IDEA to be running with MCP plugin connected.
---

# IntelliJ IDEA MCP Skill

## Critical Requirements

- **IDEA MUST be running** with MCP server connected
- **Permission dialogs** in IDEA must be accepted for commands to execute

## Path Rules (Most Common Error Source)

| Parameter | Path Type | Example |
|-----------|-----------|---------|
| `projectPath` | **ABSOLUTE** | `/Users/huangli/dt/dongting` |
| `filePath`, `pathInProject`, `directoryPath` | **RELATIVE** | `server/src/main/java/...` |

## Key Workflows

### Check Compilation Errors
```yaml
Tool: IntelliJ-IDEA_get_file_problems
Parameters:
  projectPath: "/Users/huangli/dt/dongting"
  filePath: "server/src/main/java/.../SomeFile.java"
  errorsOnly: true
```
More accurate than LSP for multi-module Maven projects.

### Run Tests
```yaml
Tool: IntelliJ-IDEA_execute_run_configuration
Parameters:
  projectPath: "/Users/huangli/dt/dongting"
  configurationName: "TtlManagerTest"
  timeout: 120000  # 2 minutes for tests
```

### Search Code
```yaml
Tool: IntelliJ-IDEA_search_in_files_by_text
Parameters:
  projectPath: "/Users/huangli/dt/dongting"
  searchText: "DtLog.getLogger"
  caseSensitive: true
```

### Safe Refactoring
1. Search first: `search_in_files_by_text` to understand scope
2. Rename: `rename_refactoring` with `symbolName` and `newName`
3. Verify: `get_file_problems` to check for errors

## Tool Selection Guide

| Task | Best Tool | Why |
|------|-----------|-----|
| Java errors/warnings | IDEA `get_file_problems` | More accurate than LSP |
| Run tests | IDEA `execute_run_configuration` | Proper test environment |
| Rename refactoring | IDEA `rename_refactoring` | Semantic, cross-file updates |
| Search code | IDEA `search_in_files_by_*` | Fast indexed search |
| Find files | IDEA `find_files_by_*` | Fast indexed search |
| Read files | LSP `read` or IDEA `get_file_text_by_path` | Either works |
| Build/compile | Bash `mvn` | More control, reproducible |
| Git operations | Bash `git` | IDEA MCP lacks git tools |

## Timeout Recommendations

| Operation | Timeout |
|-----------|---------|
| File/text search | 30000-60000 |
| Get file problems | 15000 |
| Run configuration | 120000-300000 |
| Build project | 300000 |
| Refactoring | 60000 |

## High-Risk Operations

- `execute_terminal_command`: Runs ANY shell command with full system permissions
- `rename_refactoring` & `replace_text_in_file`: Modifies code globally
- `create_new_file`: Creates files immediately
- `reformat_file`: Modifies formatting without confirmation

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Verify IDEA running with MCP plugin |
| Request timed out | Accept permission dialog in IDEA |
| File not found | Check absolute vs relative path |
| No occurrences found | Verify `projectPath` and file paths |
