---
name: idea-mcp
description: |
  IntelliJ-IDEA MCP provides powerful IDE features including running tests, code analysis, refactoring, search, and project navigation. Use this when you need accurate Java code analysis (avoiding LSP false positives), running tests via IDEA configurations, refactoring symbols, or exploring codebase structure. Key commands: execute_run_configuration (run tests), get_file_problems (accurate errors/warnings), search_in_files_by_text (search code), list_directory_tree (view structure), get_file_text_by_path (read files), rename_refactoring (safe refactoring), execute_terminal_command (run shell commands).
---

# IntelliJ IDEA MCP Skill

Provides access to IntelliJ IDEA features via ```IntelliJ-IDEA``` MCP (Model Context Protocol).

## ⚠️ Critical Requirements

### Connection
- **REQUIRES**: IntelliJ IDEA running with MCP server connected
- Commands fail if IDEA is not running

### Parameters (MUST GET RIGHT!)

**ALL commands require:**
- `projectPath`: **ABSOLUTE PATH** to project root (e.g., `/Users/huangli/dt/dongting`)
  - ❌ `dongting` (wrong - relative)
  - ✅ `/Users/huangli/dt/dongting` (correct - absolute)

**Search commands require:**
- `maxUsageCount`: **MUST** include for `search_in_files_by_text` and `search_in_files_by_regex`
  - Without this → schema error
  - Use 5-20 based on expected results

**File search commands require:**
- `fileCountLimit`: **MUST** include for `find_files_by_name_keyword` and `find_files_by_glob`
  - Without this → schema error "data must have required property 'probablyHasMoreMatchingFiles'"
  - Required by MCP server to generate pagination metadata
  - Use 10-20 based on expected results

**Path confusion:**
- `projectPath`: Always absolute
- `filePath`/`pathInProject`/`directoryPath`: Always relative to project root

## 🛡️ Safety Levels

| Command | Risk | Can |
|----------|--------|------|
| `get_file_problems`, `get_file_text_by_path`, `get_symbol_info` | Safe | Read |
| `search_in_files_by_text`, `search_in_files_by_regex`, `find_files_by_*` | Safe | Read |
| `list_directory_tree`, `get_project_modules`, `get_dependencies`, etc. | Safe | Read |
| `reformat_file` | Medium | Modify formatting |
| `rename_refactoring`, `replace_text_in_file`, `create_new_file` | High | Modify code |
| `execute_terminal_command` | High | Run ANY shell command! |

## 📚 Command Reference

### Build & Test

**`execute_run_configuration`** - Run test/build config
```json
{
  "projectPath": "/abs/path/to/project",
  "configurationName": "TestConfigName",
  "timeout": 120000
}
```
Returns: Exit code, stdout/stderr (TeamCity format)

**`get_run_configurations`** - List available configs
```json
{"projectPath": "/abs/path/to/project"}
```
Returns: Array of config names and descriptions

### Code Analysis

**`get_file_problems`** - Get errors/warnings for file
```json
{
  "projectPath": "/abs/path/to/project",
  "filePath": "relative/path/File.java",
  "errorsOnly": false
}
```
Returns: Array of errors/warnings with line numbers and messages

**`get_symbol_info`** - Get symbol documentation
```json
{
  "projectPath": "/abs/path/to/project",
  "filePath": "relative/path/File.java",
  "line": 47,
  "column": 20
}
```
Returns: Symbol name, declaration text, Javadoc if available

### Search & Navigation

**`search_in_files_by_text`** - Plain text search
```json
{
  "projectPath": "/abs/path/to/project",
  "searchText": "text to find",
  "maxUsageCount": 10
}
```
Returns: Array of matches with file path, line number, highlighted text

**`search_in_files_by_regex`** - Regex search
```json
{
  "projectPath": "/abs/path/to/project",
  "regexPattern": "public class.*Impl",
  "maxUsageCount": 10
}
```
Returns: Same format as text search

**`find_files_by_name_keyword`** - Find files by name (case-insensitive)
```json
{
  "projectPath": "/abs/path/to/project",
  "nameKeyword": "Raft",
  "fileCountLimit": 10
}
```
Returns: Array of file paths matching name keyword

**`find_files_by_glob`** - Find files by glob pattern
```json
{
  "projectPath": "/abs/path/to/project",
  "globPattern": "**/*Impl.java",
  "fileCountLimit": 20
}
```
Glob patterns: `*` (any), `**` (any dirs), `?` (single char)

**`list_directory_tree`** - View directory structure
```json
{
  "projectPath": "/abs/path/to/project",
  "directoryPath": "relative/path/to/dir",
  "maxDepth": 2
}
```
Returns: Tree representation with nested structure

### File Operations

**`get_file_text_by_path`** - Read file content
```json
{
  "projectPath": "/abs/path/to/project",
  "pathInProject": "relative/path/File.java",
  "maxLinesCount": 100,
  "truncateMode": "END"
}
```
Returns: File content with line numbers, truncated if exceeded

**`open_file_in_editor`** - Open file in IDEA (focuses window)
```json
{
  "projectPath": "/abs/path/to/project",
  "filePath": "relative/path/File.java"
}
```

**`reformat_file`** - Format file with IDEA's code style
```json
{
  "projectPath": "/abs/path/to/project",
  "path": "relative/path/File.java"
}
```
Warning: Modifies file on disk!

**`replace_text_in_file`** - Exact string replacement
```json
{
  "projectPath": "/abs/path/to/project",
  "pathInProject": "relative/path/File.java",
  "oldText": "exact text to replace",
  "newText": "replacement text",
  "replaceAll": false,
  "caseSensitive": true
}
```
Warning: Modifies file on disk!
Must match exact indentation and whitespace. Use `get_file_text_by_path` first.

**`create_new_file`** - Create new file
```json
{
  "projectPath": "/abs/path/to/project",
  "pathInProject": "relative/path/NewFile.java",
  "text": "file content",
  "overwrite": false
}
```
Warning: Creates new file! `overwrite: true` replaces existing.

### Project Intelligence

**`get_project_modules`** - List project modules
```json
{"projectPath": "/abs/path/to/project"}
```
Returns: Array of module names and types

**`get_project_dependencies`** - List project dependencies
```json
{"projectPath": "/abs/path/to/project"}
```
Returns: Array of JAR dependency names

**`get_all_open_file_paths`** - List open files
```json
{"projectPath": "/abs/path/to/project"}
```
Returns: Active file path + array of all open files

**`get_repositories`** - Get VCS (Git) info
```json
{"projectPath": "/abs/path/to/project"}
```
Returns: Array of VCS roots with paths and system name

### Terminal

**`execute_terminal_command`** - Run shell command
```json
{
  "projectPath": "/abs/path/to/project",
  "command": "shell command",
  "executeInShell": true,
  "timeout": 30000,
  "maxLinesCount": 1000
}
```
Returns: `command_output` + `command_exit_code`
Warning: Can run ANY shell command - use with caution!

### Refactoring

**`rename_refactoring`** - Rename symbol across project
```json
{
  "projectPath": "/abs/path/to/project",
  "pathInProject": "relative/path/File.java",
  "symbolName": "oldName",
  "newName": "newName"
}
```
Returns: Success message with usage count

**⚠️ Critical behavior:**
- ✅ Renames: Field/variable declarations and their references
- ❌ Does NOT rename: Method parameters in signatures, method names
- Use `search_in_files_by_text` first to understand scope
- Very dangerous! Always verify with `get_file_problems` after

## 🚨 Troubleshooting (Pitfalls & Solutions)

### Schema Error: "Structured content does not match"

**Cause**: Missing required count parameter

**For text search errors** ("maxUsageCount" related):
- Add `maxUsageCount` for `search_in_files_by_text` and `search_in_files_by_regex`
  ```json
  {"searchText":"text","maxUsageCount":10,"projectPath":"/abs/path"}
  ```

**For file search errors** ("fileCountLimit" or "probablyHasMoreMatchingFiles" related):
- Add `fileCountLimit` for `find_files_by_name_keyword` and `find_files_by_glob`
  ```json
  {"nameKeyword":"KvLockTest","fileCountLimit":10,"projectPath":"/abs/path"}
  ```
  ```json
  {"globPattern":"**/*.java","fileCountLimit":20,"projectPath":"/abs/path"}
  ```

**Why these are required**:
- Even though the tool schema shows `maxUsageCount` and `fileCountLimit` as optional
- The MCP server **requires** these parameters internally to generate pagination metadata
- Without them, the output schema validation fails (missing `probablyHasMoreMatchingFiles` field)

### Commands fail or timeout

**Cause**: IDEA not running or not connected
**Solution**:
1. Start IntelliJ IDEA
2. Ensure MCP plugin is active (check IDEA settings)
3. Wait for connection (may take a few seconds)
4. Verify project is fully loaded

### Command times out

**Cause**: Timeout too short for long operations
**Solution**:
- Increase `timeout` (in milliseconds)
- Terminal: Default 30s, may need 120-300s for builds
- Tests: May need 60-120s depending on test suite

### Search returns no results

**Cause**: Wrong path or search term
**Solution**:
1. Verify `projectPath` is absolute and correct
2. Verify file paths are relative to project root
3. Try broader search terms
4. Use `list_directory_tree` to verify structure exists

### File operation fails (create/replace/format)

**Cause**: Path wrong, file locked, or text mismatch
**Solution**:
1. Verify file path is correct and relative
2. For `create_new_file`: Use `overwrite: true` if file exists
3. For `replace_text_in_file`:
   - Use `get_file_text_by_path` first to get exact text
   - Match indentation and whitespace exactly
4. Check file is not locked by another process

## ✅ Best Practices (Quick Checklist)

1. **Always use absolute paths** for `projectPath`
2. **Use relative paths** for `filePath`/`pathInProject`/`directoryPath`
3. **Include count parameters**: `maxUsageCount` for search, `fileCountLimit` for file search
4. **Before refactoring**: Use search to understand scope first
5. **After code changes**: Use `get_file_problems` to verify no errors
6. **For Java accuracy**: Prefer IDEA's `get_file_problems` over LSP (avoids false positives)
7. **Text replacement**: Get exact text with `get_file_text_by_path` before replacing
8. **Test execution**: Use `execute_run_configuration` for proper TeamCity-formatted output
9. **Timeouts**: Build/test may need 120-300s, not default 30s

## 📋 Command Quick Reference

| Command | Risk | Read/Write | Quick Use |
|---------|--------|-------------|-------------|
| execute_run_configuration | Low | - | Run tests/builds |
| get_run_configurations | Safe | Read | List configs |
| get_file_problems | Safe | Read | Check errors |
| get_symbol_info | Safe | Read | Get symbol docs |
| search_in_files_by_text | Safe | Read | Search text |
| search_in_files_by_regex | Safe | Read | Regex search |
| find_files_by_name_keyword | Safe | Read | Find by name |
| find_files_by_glob | Safe | Read | Find by glob |
| list_directory_tree | Safe | Read | View structure |
| get_file_text_by_path | Safe | Read | Read files |
| open_file_in_editor | Safe | - | Focus IDE |
| reformat_file | Medium | Write | Format code |
| execute_terminal_command | High | - | Run shell |
| rename_refactoring | High | Write | Rename symbols |
| replace_text_in_file | High | Write | Edit files |
| create_new_file | High | Write | Create files |
| get_project_modules | Safe | Read | List modules |
| get_project_dependencies | Safe | Read | List deps |
| get_all_open_file_paths | Safe | Read | List open files |
| get_repositories | Safe | Read | Git info |
