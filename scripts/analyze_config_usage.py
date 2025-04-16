### analyze_config_usage.py
import ast
import os
import re
import argparse
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "s3_divvy", "config.py")
TARGET_DIRS = ["s3_divvy", "scripts"]

STOPWORDS = {
    "NOTE", "NOTES", "WARNING", "WARN", "IMPORTANT", "CAUTION",
    "TODO", "FIXME", "DEBUG", "HACK", "XXX",
    "YES", "NO", "OK", "DONE", "PENDING",
    "TRUE", "FALSE", "NONE", "NULL", "DEFAULT", "OPTIONAL",
    "SEE", "SECTION", "REFER", "ABOVE", "BELOW", "START", "END"
}

def extract_config_keys():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
    config_keys = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    config_keys.append(target.id)
    return config_keys

def find_usages(config_keys):
    usage_map = defaultdict(set)
    for dir_name in TARGET_DIRS:
        abs_dir = os.path.join(PROJECT_ROOT, dir_name)
        for root, _, files in os.walk(abs_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(root, fname)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        tree = ast.parse(f.read(), filename=path)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Attribute):
                            if isinstance(node.value, ast.Name) and node.value.id == "config":
                                key_name = node.attr
                                if key_name in config_keys:
                                    rel_path = os.path.relpath(path, PROJECT_ROOT)
                                    usage_map[key_name].add(rel_path)
                except Exception:
                    continue
    return usage_map

def find_comment_tokens(config_keys, mode="basic"):
    comment_map = defaultdict(set)
    potential_config = defaultdict(set)

    pattern_config = re.compile(r"config\.([A-Z_]{3,})")
    pattern_allcaps = re.compile(r"\b([A-Z_]{3,})\b")

    for dir_name in TARGET_DIRS:
        abs_dir = os.path.join(PROJECT_ROOT, dir_name)
        for root, _, files in os.walk(abs_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(root, fname)
                rel_path = os.path.relpath(path, PROJECT_ROOT)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        for line in f:
                            if "#" not in line:
                                continue
                            comment = line.split("#", 1)[1]

                            # Detect config.KEY in comments
                            for match in pattern_config.findall(comment):
                                if match in config_keys:
                                    comment_map[match].add(rel_path)

                            # Detect bare ALL_CAPS in comments
                            for match in pattern_allcaps.findall(comment):
                                if match in config_keys:
                                    comment_map[match].add(rel_path)
                                elif match not in STOPWORDS and mode != "off":
                                    potential_config[match].add(rel_path)
                except Exception:
                    continue
    return comment_map, potential_config

def print_report(code_usage, comment_usage, unknown_caps, config_keys):
    all_keys = sorted(set(config_keys) | set(code_usage) | set(comment_usage))

    print("\n🔍 Config Usage Report:\n")
    for key in all_keys:
        code_files = sorted(code_usage.get(key, []))
        comment_files = sorted(comment_usage.get(key, []))

        if code_files or comment_files:
            print(f"{key}:")
            if code_files:
                for f in code_files:
                    print(f"  • {f}")
            if comment_files:
                for f in comment_files:
                    print(f"  💬 {f}")
        else:
            print(f"{key}:\n  ⚠️ Not used")

    if unknown_caps:
        print("\n📌 Possible config candidates (ALL_CAPS in comments):\n")
        for token, files in sorted(unknown_caps.items()):
            print(f"{token}:\n  📝 " + "\n  📝 ".join(sorted(files)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--comment-scan", "-c", choices=["off", "basic", "aggressive"], default="basic")
    args = parser.parse_args()

    config_keys = extract_config_keys()
    code_usages = find_usages(config_keys)

    if args.comment_scan != "off":
        comment_usages, unknown_caps = find_comment_tokens(config_keys, mode=args.comment_scan)
    else:
        comment_usages, unknown_caps = {}, {}

    print_report(code_usages, comment_usages, unknown_caps, config_keys)