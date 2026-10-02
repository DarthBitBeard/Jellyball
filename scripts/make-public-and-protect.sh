#!/usr/bin/env bash
# Run this as the repo owner (DarthBitBeard) after `gh auth login`.
# The cloud agent token cannot change visibility or branch protection.
# HISTORICAL: already run (2026-09-30); refuses to run unless I_KNOW_THIS_IS_HISTORICAL=1.
set -euo pipefail

if [ "${I_KNOW_THIS_IS_HISTORICAL:-}" != "1" ]; then
  echo 'This one-time script already ran (2026-09-30) and is kept for history only.' >&2
  echo 'Its classic branch protection conflicts with the "Protect master" ruleset.' >&2
  echo 'Use scripts/setup-branch-protection.sh instead.' >&2
  echo 'To run this script anyway, set I_KNOW_THIS_IS_HISTORICAL=1.' >&2
  exit 1
fi

REPO="${REPO:-DarthBitBeard/Jellyball}"
BRANCH="${BRANCH:-master}"

echo "==> Making $REPO public"
gh repo edit "$REPO" --visibility public --accept-visibility-change-consequences

echo "==> Protecting $BRANCH (classic branch protection)"
# Free accounts get full protection on public repos. Rulesets also unlock once public.
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

echo "==> Creating ruleset 'Protect master' (optional duplicate of classic rules)"
gh api -X POST "repos/$REPO/rulesets" \
  -H "Accept: application/vnd.github+json" \
  --input - <<EOF || echo "(ruleset create skipped — classic protection is enough)"
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

echo "==> Done. Verify:"
gh api "repos/$REPO" --jq '{visibility, private, default_branch}'
gh api "repos/$REPO/branches/$BRANCH/protection" --jq '{
  reviews: .required_pull_request_reviews.required_approving_review_count,
  checks: .required_status_checks.contexts,
  allow_force: .allow_force_pushes.enabled,
  allow_delete: .allow_deletions.enabled
}'
echo
echo "Also hide your personal email on GitHub if needed:"
echo "  https://github.com/settings/emails  → keep 'Keep my email addresses private' checked"
