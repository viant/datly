#!/usr/bin/env bash
set -euo pipefail

repo="${1:-${DATLY_V1_REPO:-}}"
requested_ref="${2:-${DATLY_V1_REF:-}}"

if [[ -z "$repo" ]]; then
  gopath="$(go env GOPATH 2>/dev/null || true)"
  if [[ -n "$gopath" ]]; then
    candidate="${gopath%%:*}/src/github.com/viant/datly"
    if [[ -d "$candidate/.git" ]]; then
      repo="$candidate"
    fi
  fi
fi

if [[ -z "$repo" || ! -d "$repo/.git" ]]; then
  echo "Datly repository not found; pass its path or set DATLY_V1_REPO" >&2
  exit 1
fi

required=(
  "spec/component.go"
  "bootstrap/artifact.go"
  "transcribe/compiler.go"
  "application/generation.go"
  "llm/datly-reader/SKILL.md"
  "llm/datly-writer/SKILL.md"
  "llm/datly-custom-component/SKILL.md"
)

tree_has_v1() {
  local ref="$1"
  local path
  for path in "${required[@]}"; do
    if [[ "$ref" == "WORKTREE" ]]; then
      [[ -f "$repo/$path" ]] || return 1
    else
      git -C "$repo" cat-file -e "$ref:$path" 2>/dev/null || return 1
    fi
  done
}

ref="$requested_ref"
if [[ -z "$ref" ]]; then
  branch="$(git -C "$repo" branch --show-current 2>/dev/null || true)"
  if [[ "$branch" == "v1" ]] && tree_has_v1 WORKTREE; then
    ref=WORKTREE
  elif tree_has_v1 origin/v1; then
    ref=origin/v1
  elif tree_has_v1 v1; then
    ref=v1
  else
    echo "No canonical Datly 1.0 tree found in worktree, origin/v1, or v1" >&2
    exit 1
  fi
elif [[ "$ref" == "WORKTREE" ]]; then
  branch="$(git -C "$repo" branch --show-current 2>/dev/null || true)"
  if [[ "$branch" != "v1" ]]; then
    echo "WORKTREE is allowed only when the checked-out branch is v1" >&2
    exit 1
  fi
  tree_has_v1 WORKTREE || { echo "worktree is not a complete Datly 1.0 tree" >&2; exit 1; }
else
  tree_has_v1 "$ref" || { echo "$ref is not a complete Datly 1.0 tree" >&2; exit 1; }
fi

echo "DATLY_V1_REPO=$repo"
echo "DATLY_V1_REF=$ref"
for skill in datly-reader datly-writer datly-custom-component; do
  echo "DATLY_V1_SKILL=llm/$skill/SKILL.md"
done
