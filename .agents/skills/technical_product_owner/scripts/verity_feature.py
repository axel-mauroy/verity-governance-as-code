#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime

# Path relative to project root
SKILL_PATH = ".skills/technical_product_owner/SKILL.md"
DOCS_DIR = "docs/features"

def main():
    parser = argparse.ArgumentParser(description="Verity Feature Workflow Helper")
    parser.add_argument("request", nargs="+", help="Description of the feature")
    args = parser.parse_args()

    feature_request = " ".join(args.request)
    skill_context = read_skill_context()
    
    print("\n" + "="*60)
    print("🤖  ANTIGRAVITY AGENT CONTEXT GENERATOR  🤖")
    print("="*60)
    print("\n>>> INSTRUCTION FOR AGENT <<<")
    print(f"I want to plan a new feature: '{feature_request}'")
    print("\nPlease act as the 'Feature Workflow Agent' defined below and generate the 'Feature Bible' document.")
    print("\n--- SKILL DEFINITION ---")
    print(skill_context)
    print("\n--- END OF SKILL DEFINITION ---")
    print("\n>>> ACTION REQUIRED <<<")
    print(f"1. Analyze the request '{feature_request}'")
    print(f"2. Simulate the 'Council' as described in the Workflow.")
    print(f"3. Generate the Feature Design Document (FDD).")
    print(f"4. Save it as a markdown file in '{DOCS_DIR}/YYYYMMDD_feature_name.md'")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()