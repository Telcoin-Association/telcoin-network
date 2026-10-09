"""Identify the real GitHub Actions executor without a local thread claim."""
import os
import re


def runner_identity(environment=None):
    values = os.environ if environment is None else environment
    if values.get('GITHUB_ACTIONS') != 'true':
        raise ValueError('proof generation requires the actual GitHub Actions environment')
    repository = values.get('GITHUB_REPOSITORY', '')
    run = values.get('GITHUB_RUN_ID', '')
    attempt = values.get('GITHUB_RUN_ATTEMPT', '')
    job = values.get('GITHUB_JOB', '')
    if (not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository)
            or not re.fullmatch(r'[1-9][0-9]*', run)
            or not re.fullmatch(r'[1-9][0-9]*', attempt)
            or not re.fullmatch(r'[A-Za-z0-9_-]+', job)):
        raise ValueError('GitHub Actions executor identity is incomplete or malformed')
    return f'github-actions/{repository}/{run}/attempt{attempt}/job{job}'
