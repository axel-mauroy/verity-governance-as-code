#!/usr/bin/env python3
import os
import sys
import argparse

TEMPLATE = """---
name: {human_name}
description: [Short description of the skill]
---

# {human_name}

[Detailed description of what this skill does and how to use it]

## Agent Context
> [!IMPORTANT]
> This section defines what this agent knows about the project ecosystem.
- **Role**: [What is the specific role of this agent?]
- **Scope**: [What files/systems does it touch? What does it NOT touch?]
- **Conventions**: [Specific coding styles or patterns relevant to this skill]

## Interactions
- **Inputs**: [What initiates this skill? User prompt? Other agent?]
- **Outputs**: [What does it produce? Code? Plan? Report?]
- **Collaborators**: [Does it need to call other agents/skills?]

## Prerequisites
- [List any tools or dependencies required]

## Usage
Example usage:
```bash
# bash command
```
"""

def create_skill(skill_name, base_path=".skills"):
    # Convert snake_case to Human Readable Title Case for the template
    human_name = skill_name.replace("_", " ").title()
    
    skill_dir = os.path.join(base_path, skill_name)
    scripts_dir = os.path.join(skill_dir, "scripts")
    
    if os.path.exists(skill_dir):
        print(f"Error: Skill '{skill_name}' already exists at {skill_dir}")
        sys.exit(1)
        
    try:
        os.makedirs(scripts_dir)
        print(f"Created directory: {skill_dir}")
        print(f"Created directory: {scripts_dir}")
        
        skill_md_path = os.path.join(skill_dir, "SKILL.md")
        with open(skill_md_path, "w") as f:
            f.write(TEMPLATE.format(human_name=human_name))
        print(f"Created file: {skill_md_path}")
        
        print(f"\nSkill '{skill_name}' created successfully!")
        
    except Exception as e:
        print(f"Error creating skill: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Create a new Agent Skill")
    parser.add_argument("skill_name", help="Name of the skill (snake_case suggested)")
    args = parser.parse_args()
    
    # Ensure we are running from project root or close enough
    if not os.path.exists(".skills") and os.path.exists("../.skills"):
        os.chdir("..")
        
    if not os.path.exists(".skills"):
        # try creating it if it doesn't exist (first run)
        try:
            os.makedirs(".skills")
        except:
             print("Error: Could not find or create .skills directory. Please run from project root.")
             sys.exit(1)

    create_skill(args.skill_name)

if __name__ == "__main__":
    main()
