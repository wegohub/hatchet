#!/usr/bin/env bash
set -euo pipefail

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

command -v gh >/dev/null 2>&1 || fail 'Install GitHub CLI (gh) and run gh auth login first.'
command -v python3 >/dev/null 2>&1 || fail 'Install Python 3 to compare semantic version tags.'
cd "$(git rev-parse --show-toplevel)"
[[ "$(git branch --show-current)" == main ]] || fail 'Switch to main before running task wego.'
[[ -z "$(git status --porcelain)" ]] || fail 'Commit or stash local changes before running task wego.'

# Check the configured URL without printing credentials embedded in the remote.
case "$(git config --get remote.origin.url)" in
    https://github.com/wegohub/hatchet|https://github.com/wegohub/hatchet.git|\
    https://*@github.com/wegohub/hatchet|https://*@github.com/wegohub/hatchet.git|\
    git@github.com:wegohub/hatchet|git@github.com:wegohub/hatchet.git|\
    ssh://git@github.com/wegohub/hatchet|ssh://git@github.com/wegohub/hatchet.git) ;;
    *) fail 'origin must point to github.com/wegohub/hatchet.' ;;
esac

# A fast-forward pull permits local-only commits; reject those before remote sync.
git fetch --no-tags origin refs/heads/main:refs/remotes/origin/main
git merge-base --is-ancestor HEAD refs/remotes/origin/main ||
    fail 'Local main has unpushed or divergent commits; reconcile it with origin/main first.'

printf '[1/3] Syncing fork main from hatchet-dev/hatchet\n'
GH_HOST=github.com gh repo sync wegohub/hatchet --source hatchet-dev/hatchet --branch main

printf '[2/3] Pulling fork main\n'
git -c fetch.pruneTags=false pull --ff-only --no-tags origin main
main_commit=$(git rev-parse HEAD)
[[ "$main_commit" == "$(git rev-parse refs/remotes/origin/main)" ]] ||
    fail 'Local main does not match origin/main.'

printf '[3/3] Finding the highest upstream semantic version tag (including prereleases)\n'
tag=$(
    git ls-remote --tags --refs https://github.com/hatchet-dev/hatchet.git |
        python3 ./hack/dev/latest-semver-tag.py
)

remote_commit=$(
    git ls-remote --tags origin "refs/tags/$tag" "refs/tags/$tag^{}" |
        awk '/\^\{\}$/ { peeled=$1; next } { direct=$1 }
            END { if (peeled != "") print peeled; else if (direct != "") print direct }'
)
if [[ -n "$remote_commit" && "$remote_commit" != "$main_commit" ]]; then
    fail "Fork tag $tag already points to another commit; refusing to overwrite it."
fi

if git show-ref --verify --quiet "refs/tags/$tag"; then
    [[ "$(git rev-parse "refs/tags/$tag^{commit}")" == "$main_commit" ]] ||
        fail "Local tag $tag already points to another commit; refusing to overwrite it."
else
    # The tag names a fork main snapshot, rather than copying the upstream tag's target.
    git tag --no-sign "$tag" "$main_commit"
fi

if [[ -z "$remote_commit" ]]; then
    git push origin "refs/tags/$tag:refs/tags/$tag"
else
    printf 'Fork tag %s already points to main; nothing to push.\n' "$tag"
fi
printf 'Done: %s -> %s\n' "$tag" "$main_commit"
