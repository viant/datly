Here’s the updated **README.md** with paragraph 8 converted into a quick-reference table for clarity:

---

# Datler AI Agent

**Datler** is an AI-powered tool for generating `.dql` files.

The `agently_root` folder contains the Agently workspace for the Datler AI Agent.
It is configured to work with the **SQLKit MCP server**, enabling database interactions.
By default, Datler uses the `o3-model`, but you can configure it to use other models as well.

---

## How to Start

1. **Install Agently and the MCP server**
   Follow the installation instructions in [github.com/viant/agently/README.md](https://github.com/viant/agently/blob/main/README.md).

2. **Set the `AGENTLY_ROOT` environment variable** to your desired workspace directory:

   ```bash
   export AGENTLY_ROOT=[datly_project_path]/agently/agently_root
   ```

3. **Run Agently**:

   ```bash
   agently serve
   ```

4. **Access Datler** in your browser:

   ```
   http://localhost:8080/
   ```

5. **Specify your project folder**
   Datler will ask for the location of your project folder, where it will create `.dql` files.

6. **Provide database connection parameters when needed**
   If a task requires database access, Datler will prompt you for the connection details.

7. **Run examples**

    * Create the example tables using `test/schema.sql`.
    * Use prompts from `agently/test/prompts.txt`.

8. **Edit knowledge and system knowledge**

| Folder                                        | Purpose                                                                                                  | Included In  | Notes                                                  |
|-----------------------------------------------| -------------------------------------------------------------------------------------------------------- | ------------ | ------------------------------------------------------ |
| `agently_root/agents/datlerknowledge/`        | Task-specific or contextual knowledge: instructions, templates, schemas, definitions, validation rules.  | `user` msg   | Should **not** contain general behavior or tone rules. |
| `agently_root/agents/datlersystem_knowledge/` | System-level knowledge: default behavior, formatting rules, tone, safety boundaries, output constraints. | `system` msg | Applies to **all** tasks and overrides defaults.       |

---
