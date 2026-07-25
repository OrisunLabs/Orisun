#!/bin/bash

# This script helps with creating a new release of Orisun

set -e

usage() {
    cat <<EOF
Usage: $0 <version> [--notes <file>]

Examples:
  $0 1.0.0
  $0 1.0.0 --notes release-notes.md

Notes:
  When --notes is provided, the file becomes the annotated tag message.
  Markdown headings are preserved exactly in the tag message.
  The GitHub release workflow uses that tag message as the release notes.
  The script snapshots, validates, commits, and pushes the versioned docs before
  creating the tag so GitHub Pages publishes the same release.
  After pushing the tag, the script asks proxy.golang.org and pkg.go.dev to
  fetch the new module version. Set SKIP_GO_INDEX_SYNC=1 to skip that step.
EOF
}

warn() {
    echo "Warning: $*" >&2
}

module_path() {
    sed -n 's/^module[[:space:]]\{1,\}//p' go.mod | head -n 1
}

sync_go_index() {
    if [ "${SKIP_GO_INDEX_SYNC:-}" = "1" ]; then
        echo "Skipping Go proxy/pkg.go.dev sync because SKIP_GO_INDEX_SYNC=1."
        return 0
    fi

    local module version_tag proxy_synced fetch_url status
    module=$(module_path)
    version_tag="v$VERSION"

    if [ -z "$module" ]; then
        warn "Could not determine module path from go.mod; skipping Go index sync."
        return 0
    fi

    echo "Syncing Go module proxy for $module@$version_tag..."
    proxy_synced=0
    if command -v go >/dev/null 2>&1; then
        for attempt in 1 2 3; do
            if GOPROXY=https://proxy.golang.org go list -m "$module@$version_tag" >/dev/null 2>&1; then
                proxy_synced=1
                echo "Go module proxy resolved $module@$version_tag."
                break
            fi

            if [ "$attempt" -lt 3 ]; then
                echo "Go proxy sync attempt $attempt failed; retrying..."
                sleep $((attempt * 10))
            fi
        done

        if [ "$proxy_synced" -ne 1 ]; then
            warn "Go proxy did not resolve $module@$version_tag yet. It may index shortly."
        fi
    else
        warn "go command not found; skipping Go proxy sync."
    fi

    if command -v curl >/dev/null 2>&1; then
        fetch_url="https://pkg.go.dev/fetch/$module@$version_tag"
        echo "Requesting pkg.go.dev fetch for $module@$version_tag..."

        for attempt in 1 2 3; do
            status=$(curl -fsS -o /dev/null -w "%{http_code}" "$fetch_url" 2>/dev/null || true)
            if [[ "$status" =~ ^[23] ]]; then
                echo "pkg.go.dev accepted fetch request for $module@$version_tag."
                break
            fi

            if [ "$attempt" -lt 3 ]; then
                echo "pkg.go.dev fetch attempt $attempt returned ${status:-no response}; retrying..."
                sleep $((attempt * 10))
            else
                warn "pkg.go.dev fetch returned ${status:-no response}. The page may still render shortly."
            fi
        done
    else
        warn "curl command not found; skipping pkg.go.dev fetch request."
    fi

    echo "Go package docs: https://pkg.go.dev/$module@$version_tag"
}

prepare_versioned_docs() {
    local versions_file config_file versioned_docs_dir versioned_sidebar tmp_file
    versions_file="docs/versions.json"
    config_file="docs/docusaurus.config.ts"
    versioned_docs_dir="docs/versioned_docs/version-$VERSION"
    versioned_sidebar="docs/versioned_sidebars/version-$VERSION-sidebars.json"

    if ! grep -Fq "\"$VERSION\"" "$versions_file"; then
        if ! command -v bun >/dev/null 2>&1; then
            echo "Error: Bun is required to create the versioned documentation snapshot."
            exit 1
        fi

        echo "Creating documentation snapshot for v$VERSION..."
        (
            cd docs
            bun install --frozen-lockfile
            bun x docusaurus docs:version "$VERSION"
        )
    fi

    if ! grep -Fq "lastVersion: '$VERSION'" "$config_file"; then
        tmp_file=$(mktemp)
        awk -v version="$VERSION" '
            {
                sub(/lastVersion: ['"'"'"][^'"'"'"]+['"'"'"]/, "lastVersion: \047" version "\047")
                print
            }
        ' "$config_file" > "$tmp_file"
        mv "$tmp_file" "$config_file"
    fi

    if [ -n "$(git status --porcelain -- docs)" ]; then
        if ! command -v bun >/dev/null 2>&1; then
            echo "Error: Bun is required to validate the versioned documentation."
            exit 1
        fi

        echo "Validating documentation snapshot for v$VERSION..."
        (
            cd docs
            bun install --frozen-lockfile
            bun run build
        )

        git add \
            "$versions_file" \
            "$config_file" \
            "$versioned_docs_dir" \
            "$versioned_sidebar"
        git commit -m "docs: publish v$VERSION documentation"
        echo "Pushing versioned documentation to main..."
        git push origin main
    else
        echo "Documentation snapshot for v$VERSION is already committed."
    fi
}

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    usage
    exit 0
fi

# Check if a version argument was provided
if [ -z "$1" ]; then
    echo "Error: No version specified"
    usage
    exit 1
fi

VERSION="$1"
NOTES_FILE=""
shift

while [ $# -gt 0 ]; do
    case "$1" in
        --notes|-n)
            if [ -z "$2" ]; then
                echo "Error: --notes requires a file path"
                usage
                exit 1
            fi
            NOTES_FILE="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "Error: Unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate version format (should be semver without 'v' prefix)
if [[ ! $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9\.]+)?$ ]]; then
    echo "Error: Version must be in semver format (e.g., 1.0.0, 1.2.3-beta.1)"
    exit 1
fi

if [ -n "$NOTES_FILE" ]; then
    if [ ! -f "$NOTES_FILE" ]; then
        echo "Error: Release notes file not found: $NOTES_FILE"
        exit 1
    fi
    if [ ! -s "$NOTES_FILE" ]; then
        echo "Error: Release notes file is empty: $NOTES_FILE"
        exit 1
    fi
fi

# Ensure we're on the main branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "Error: You must be on the main branch to create a release"
    exit 1
fi

# Ensure the working directory is clean
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Working directory is not clean. Commit or stash changes before creating a release."
    exit 1
fi

# Pull latest changes
echo "Pulling latest changes from main..."
git pull origin main

# Cut, validate, commit, and publish the documentation snapshot before tagging
# the same commit. This also triggers the Pages workflow through the docs paths.
prepare_versioned_docs

# Create and push the tag
echo "Creating tag v$VERSION..."
if [ -n "$NOTES_FILE" ]; then
    git tag -a "v$VERSION" --cleanup=verbatim -F "$NOTES_FILE"
else
    git tag -a "v$VERSION" -m "Release v$VERSION"
fi

echo "Pushing tag to remote..."
git push origin "v$VERSION"

sync_go_index

printf "\nRelease v%s has been tagged and pushed.\n" "$VERSION"
if [ -n "$NOTES_FILE" ]; then
    echo "Release notes were attached from: $NOTES_FILE"
else
    echo "No release notes file was provided; the workflow will generate notes from commits."
fi
echo "The GitHub Actions workflow will now build and publish the release."
echo "You can monitor the progress at: https://github.com/$(git config --get remote.origin.url | sed 's/.*github.com[:\/]\(.*\)\.git/\1/')/actions"
