---
name: skill-creator
description: Acts as a Meta-Skill to create other Skills. Use when you need to scaffold a new `.agents/skills` repository component.
---

# Skill Creator

This skill provides utilities to generate new Agent Skills consistent with the project's standards.

## When to use this skill

- Use this when the user requires you to add or scaffold a brand new skill pattern.
- This is helpful for auto-generating the boilerplate and executing the required `.skills/skill_creator/scripts/new_skill.py`.

## How to use it

Follow the instructions below carefully:

## Usage

To create a new skill, run the script using `uv`:

```bash
uv run .skills/skill_creator/scripts/new_skill.py <skill_name>
```

## Arguments

- `<skill_name>`: The name of the skill to create. Should be in snake_case (e.g., `feature_scaffold`, `db_migration`).

## Output

The script will create:
- `.skills/<skill_name>/`
- `.skills/<skill_name>/SKILL.md` (with template content)
- `.skills/<skill_name>/scripts/` (empty)

## Defining Agent Context

When a new skill is created, it is crucial to fill the **Agent Context** section in the generated `SKILL.md`. This acts as the "Prompt Context" for the AI agent using this skill.

### What to include:
1.  **Architecture Limits**: Explicitly state if the agent is allowed to change architecture or just implementation.
2.  **Style Guide**: Reference specific project guidelines (e.g., "Use Rust 2024 edition", "Prefer async/await").
3.  **Knowledge Graph**: Explain how this component fits into the `verity` ecosystem.

> [!TIP]
> Use `uv` for all python script executions to ensure reproducible environments.

