#!/usr/bin/env python3
"""Summarize GitHub PR review state with thread-level resolution data."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from typing import Any


QUERY = """
query($owner:String!,$repo:String!,$number:Int!,$cursor:String) {
  repository(owner:$owner,name:$repo) {
    pullRequest(number:$number) {
      number
      title
      url
      state
      comments(first: 100) {
        totalCount
        nodes {
          id
          author { login }
          createdAt
          updatedAt
          body
          url
        }
      }
      reviews(first: 100) {
        totalCount
        nodes {
          id
          state
          author { login }
          submittedAt
          body
        }
      }
      reviewThreads(first: 100, after:$cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          originalLine
          resolvedBy { login }
          comments(first: 20) {
            nodes {
              id
              author { login }
              createdAt
              updatedAt
              body
              url
            }
          }
        }
      }
    }
  }
}
"""

REVIEWS_QUERY = """
query($owner:String!,$repo:String!,$number:Int!,$cursor:String) {
  repository(owner:$owner,name:$repo) {
    pullRequest(number:$number) {
      reviews(first: 100, after:$cursor) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          state
          author { login }
          submittedAt
          body
        }
      }
    }
  }
}
"""


def run_gh(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    command = ["gh", "api", "graphql", "-f", f"query={query}"]
    for key, value in variables.items():
        if value is None:
            continue
        command.extend(["-F", f"{key}={value}"])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def require_pr(payload: dict[str, Any], owner: str, repo: str, number: int) -> dict[str, Any]:
    repository = payload.get("data", {}).get("repository")
    if repository is None:
        raise ValueError(f"repository not found: {owner}/{repo}")
    pull_request = repository.get("pullRequest")
    if pull_request is None:
        raise ValueError(f"pull request not found: {owner}/{repo}#{number}")
    return pull_request


def fetch_reviews(owner: str, repo: str, number: int) -> dict[str, Any]:
    cursor = None
    total_count = 0
    reviews: list[dict[str, Any]] = []
    while True:
        payload = run_gh(
            REVIEWS_QUERY,
            {"owner": owner, "repo": repo, "number": number, "cursor": cursor},
        )
        current = require_pr(payload, owner, repo, number)
        page = current["reviews"]
        total_count = page["totalCount"]
        reviews.extend(review for review in page["nodes"] if review is not None)
        page_info = page["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    return {"totalCount": total_count, "nodes": reviews}


def fetch_pr(owner: str, repo: str, number: int) -> dict[str, Any]:
    cursor = None
    pull_request = None
    threads: list[dict[str, Any]] = []
    while True:
        payload = run_gh(
            QUERY,
            {"owner": owner, "repo": repo, "number": number, "cursor": cursor},
        )
        current = require_pr(payload, owner, repo, number)
        if pull_request is None:
            pull_request = current
        threads.extend(current["reviewThreads"]["nodes"])
        page_info = current["reviewThreads"]["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]

    assert pull_request is not None
    pull_request["reviewThreads"]["nodes"] = threads
    pull_request["reviews"] = fetch_reviews(owner, repo, number)
    return pull_request


def one_line(text: str, limit: int = 180) -> str:
    collapsed = " ".join(text.split())
    return collapsed if len(collapsed) <= limit else collapsed[: limit - 1] + "..."


def first_thread_comment(thread: dict[str, Any]) -> dict[str, Any] | None:
    comments = thread["comments"]["nodes"]
    return comments[0] if comments else None


def thread_comment_author(thread: dict[str, Any]) -> str:
    first = first_thread_comment(thread)
    if first is None:
        return "unknown"
    author = first.get("author")
    if author is None:
        return "ghost"
    return author.get("login") or "ghost"


def thread_comment_body(thread: dict[str, Any], limit: int = 180) -> str:
    first = first_thread_comment(thread)
    if first is None:
        return "<no comments>"
    return one_line(first["body"], limit)


def render_text(pr: dict[str, Any], show_all: bool) -> str:
    threads = pr["reviewThreads"]["nodes"]
    unresolved = [thread for thread in threads if not thread["isResolved"]]
    resolved = [thread for thread in threads if thread["isResolved"]]
    outdated = [thread for thread in threads if thread["isOutdated"]]
    review_states = Counter(review["state"] for review in pr["reviews"]["nodes"])
    thread_authors = Counter(thread_comment_author(thread) for thread in threads)

    lines = [
        f"PR #{pr['number']}: {pr['title']}",
        f"URL: {pr['url']}",
        f"State: {pr['state']}",
        "",
        f"Conversation comments: {pr['comments']['totalCount']}",
        f"Reviews: {pr['reviews']['totalCount']} {dict(sorted(review_states.items()))}",
        (
            "Review threads: "
            f"total={len(threads)} unresolved={len(unresolved)} "
            f"resolved={len(resolved)} outdated={len(outdated)}"
        ),
        f"Thread authors: {dict(sorted(thread_authors.items()))}",
        "",
        "Unresolved threads:",
    ]

    if not unresolved:
        lines.append("- none")
    for thread in unresolved:
        lines.append(
            "- "
            f"{thread['id']} {thread['path']}:{thread.get('line') or thread.get('originalLine')} "
            f"outdated={thread['isOutdated']} author={thread_comment_author(thread)} "
            f"body={thread_comment_body(thread)}"
        )

    if show_all:
        lines.extend(["", "All review threads:"])
        for thread in threads:
            lines.append(
                "- "
                f"{thread['id']} resolved={thread['isResolved']} "
                f"outdated={thread['isOutdated']} {thread['path']}:"
                f"{thread.get('line') or thread.get('originalLine')} "
                f"author={thread_comment_author(thread)} body={thread_comment_body(thread, 140)}"
            )

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="GitHub repo as owner/name")
    parser.add_argument("--pr", required=True, type=int, help="Pull request number")
    parser.add_argument("--all", action="store_true", help="Print all review threads")
    parser.add_argument("--json", action="store_true", help="Emit raw normalized JSON")
    args = parser.parse_args()

    try:
        owner, repo = args.repo.split("/", 1)
    except ValueError:
        parser.error("--repo must be formatted as owner/name")

    pr = fetch_pr(owner, repo, args.pr)
    if args.json:
        print(json.dumps(pr, indent=2, sort_keys=True))
    else:
        print(render_text(pr, args.all))
    return 0


if __name__ == "__main__":
    sys.exit(main())
