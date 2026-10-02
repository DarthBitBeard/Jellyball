#!/usr/bin/env bash
# Owner-only: protect master for PR-based community collaboration.
# Requires: gh auth login as DarthBitBeard (admin on the repo).
#
# Model:
#   - Anyone can clone / fork (repo is public)
#   - Contributors push to their forks and open PRs
#   - master accepts merges only through a pull request that has a review,
#     linear history and green CI (rules in .github/rulesets/protect-master.json)
#   - Repository admins can bypass the rules, so a sole maintainer is never
#     locked out of their own repository
#
# This applies the "Protect master" ruleset only. It deliberately does NOT turn
# on classic branch protection: its `enforce_admins` and strict-status-check
# options would lock a sole maintainer out and would conflict with the ruleset.
set -euo pipefail

REPO="${REPO:-DarthBitBeard/Jellyball}"
BRANCH="${BRANCH:-master}"
RULESET_FILE="$(cd "$(dirname "$0")/.." && pwd)/.github/rulesets/protect-master.json"

echo "==> Collaboration-friendly repo settings"
gh repo edit "$REPO" \
  --allow-forking \
  --enable-issues \
  --delete-branch-on-merge \
  --allow-squash-merge \
  --allow-rebase-merge \
  --deny-merge-commit

echo "==> Ruleset 'Protect master' from $RULESET_FILE"
existing_id="$(gh api "repos/$REPO/rulesets" --jq '.[] | select(.name == "Protect master") | .id' | head -n1)"
if [ -n "$existing_id" ]; then
  echo "    updating existing ruleset $existing_id"
  gh api -X PUT "repos/$REPO/rulesets/$existing_id" \
    -H "Accept: application/vnd.github+json" \
    --input "$RULESET_FILE" \
    --jq '{id, name, enforcement}'
else
  echo "    creating it"
  gh api -X POST "repos/$REPO/rulesets" \
    -H "Accept: application/vnd.github+json" \
    --input "$RULESET_FILE" \
    --jq '{id, name, enforcement}'
fi

echo
echo "==> Verify"
gh api "repos/$REPO" --jq '{visibility, delete_branch_on_merge, allow_forking, default_branch}'
gh api "repos/$REPO/rulesets" --jq '.[] | {id, name, enforcement, target}'
echo
echo "Community workflow: fork -> branch -> PR into ${BRANCH}."
echo "Optional: Settings -> Collaborators to give trusted people write access"
echo "(they still cannot push straight to ${BRANCH} while the ruleset is active)."
echo
echo "UI alternative: Settings -> Rules -> Rulesets -> New ruleset -> Import a ruleset,"
echo "then choose .github/rulesets/protect-master.json (pull request with one review"
echo "and code-owner review, linear history, checks: test (windows-latest) and"
echo "test (ubuntu-latest), admin bypass)."
