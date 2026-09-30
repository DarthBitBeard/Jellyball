#!/usr/bin/env bash
# Owner-only: protect master for PR-based community collaboration.
# Requires: gh auth login as DarthBitBeard (admin on the repo).
#
# Model:
#   - Anyone can clone / fork (repo is public)
#   - Contributors push to their forks and open PRs
#   - master accepts merges only via reviewed PRs with green CI
set -euo pipefail

REPO="${REPO:-DarthBitBeard/Jellyball}"
BRANCH="${BRANCH:-master}"

echo "==> Collaboration-friendly repo settings"
gh repo edit "$REPO" \
  --allow-forking \
  --enable-issues \
  --delete-branch-on-merge \
  --allow-squash-merge \
  --allow-rebase-merge \
  --deny-merge-commit

echo "==> Protecting $BRANCH (classic branch protection)"
gh api -X PUT "repos/$REPO/branches/$BRANCH/protection" \
  -H "Accept: application/vnd.github+json" \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "test (windows-latest)",
      "test (ubuntu-latest)",
      "e2e",
      "docker-smoke"
    ]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false
}
EOF

echo "==> Ruleset 'Protect master' (same rules; fine if classic already applied)"
gh api -X POST "repos/$REPO/rulesets" \
  -H "Accept: application/vnd.github+json" \
  --input - <<EOF || echo "(ruleset skipped — classic protection is enough if the PUT above succeeded)"
{
  "name": "Protect master",
  "target": "branch",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["refs/heads/${BRANCH}"],
      "exclude": []
    }
  },
  "rules": [
    {"type": "deletion"},
    {"type": "non_fast_forward"},
    {"type": "pull_request", "parameters": {
      "required_approving_review_count": 1,
      "dismiss_stale_reviews_on_push": true,
      "require_code_owner_review": false,
      "require_last_push_approval": false,
      "required_review_thread_resolution": false
    }},
    {"type": "required_status_checks", "parameters": {
      "strict_required_status_checks_policy": true,
      "required_status_checks": [
        {"context": "test (windows-latest)"},
        {"context": "test (ubuntu-latest)"},
        {"context": "e2e"},
        {"context": "docker-smoke"}
      ]
    }}
  ]
}
EOF

echo
echo "==> Verify"
gh api "repos/$REPO" --jq '{visibility, delete_branch_on_merge, allow_forking, default_branch}'
gh api "repos/$REPO/branches/$BRANCH/protection" --jq '{
  reviews: .required_pull_request_reviews.required_approving_review_count,
  checks: .required_status_checks.contexts,
  enforce_admins: .enforce_admins.enabled,
  allow_force: .allow_force_pushes.enabled,
  allow_delete: .allow_deletions.enabled
}'
echo
echo "Community workflow: fork → branch → PR into ${BRANCH}."
echo "Optional: Settings → Collaborators to give trusted people write access"
echo "(they still cannot push straight to ${BRANCH} while protection is on)."
echo
echo "UI alternative: Settings → Rules → Rulesets → New branch ruleset"
echo "  Target: ${BRANCH}"
echo "  Require pull request (1 approval), block force pushes/deletions,"
echo "  require status checks: test (windows-latest), test (ubuntu-latest), e2e, docker-smoke"
