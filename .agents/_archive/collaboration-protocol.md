---
description: Standardized rules for the optimized planning, routing, context hygiene, and execution workflow.
trigger: always_on
---

# AI Collaboration & Task Execution Protocol

This document defines the optimized workflow process for articulating and executing tasks. Follow these steps for maximum efficiency and context hygiene.

## Step 1: Workspace Rules Configuration
- **Foundations First**: Set the foundation before coding to prevent the AI from guessing intentions.
- **Local Rules**: Define workspace rules specifying the stack, code style preferences, and project architecture.
- **Skills & Workflows**: Leverage documented reusable instructions (Skills/Workflows) for recurring tasks like Code Reviews, Testing, or Optimizations.

## Step 2: Planning Phase (Planning Mode)
- **No Monolithic Prompts**: Never ask "build my app" in a single go.
- **Analyze**: Always inspect the existing repository structure first.
- **Cluster & Divide**: Branch the work by creating an implementation plan divided into logical, independent clusters aligned with the project's agent skills:
  - **Rust Core & Architecture** (C1, C2...) - *Rust Systems Engineer*
  - **Security & Governance** (G1, G2...) - *Governance Architect*
  - **Query Engine & Arrow** (Q1, Q2...) - *Query Engine Specialist*
  - **Machine Learning & Vectors** (M1, M2...) - *ML Engineer*
  - **Developer Experience & CLI** (D1, D2...) - *DX Specialist*
  - **Testing & Docs-as-Code** (T1, T2...) - *QA Engineer*
  - **Specs & Product Planning** (P1, P2...) - *Technical Product Owner*

## Step 3: Intelligent Routing (Model & Mode)
Match the tool format to the difficulty of the task:
- **Complex Tasks** (Architecture, Deep Debugging, Business Logic): Use **Planning Mode** to establish the "map".
- **Simple Tasks** (Renaming, UI Fixes, Linting): Use **Fast Mode** to walk the "path".

## Step 4: Context Hygiene
- **One Conversation = One Work Stream**: Modularity is key. Keep backend work in one chat and frontend in another to prevent contextual confusion.
- **The Handoff**: If a conversation gets too long and slow, end it. Open a new conversation with a clear summary of the current state (e.g., "B1 & B2 complete. DB Schema fixed. Focus entirely on F1. Do not touch Auth.").
- **Direct Anchoring**: Don't broadly describe errors. Directly paste terminal logs or point to the specific file/symbol involved.

## Step 5: Parallel Execution & Continuous Feedback
- **Parallelism**: If tasks are strictly independent (e.g., B1 and F1), execute them concurrently in separate threads.
- **Short Feedback Loops**:
  - Review the Plan *before* generating code.
  - Review the Diff *before* applying changes.
  - Rely on generated Artifacts to instantly correct the trajectory.

## Executive Summary
1. **Plan** meticulously using logical, skill-aligned clusters (e.g., C1, G1, Q1, M1).
2. **Execute** minor tasks swiftly using Fast Mode.
3. **Isolate** workflows into clean, focused conversations.
4. **Verify** via structured tests and reviews before finalizing.
