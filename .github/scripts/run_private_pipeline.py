from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


WORKSPACE = Path(os.environ['GITHUB_WORKSPACE'])
RUNNER_TEMP = Path(os.environ['RUNNER_TEMP'])
EVENT_PATH = Path(os.environ['GITHUB_EVENT_PATH'])
PRIVATE_DIR = WORKSPACE / 'private-repo'
TEMP_LOG_DIR = RUNNER_TEMP / 'private-pipeline-logs'


class PipelineError(RuntimeError):
    pass


def _sanitize(text: str, secrets: list[str]) -> str:
    result = text
    for secret in secrets:
        if secret:
            result = result.replace(secret, '***')
    return result


def _write_log(name: str, content: str, *, secrets: list[str]) -> None:
    TEMP_LOG_DIR.mkdir(parents=True, exist_ok=True)
    (TEMP_LOG_DIR / name).write_text(_sanitize(content, secrets), encoding='utf-8', errors='replace')


def _run(
    name: str,
    args: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    secrets: list[str],
    stdout_file: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    completed = subprocess.run(
        args,
        cwd=cwd,
        env=merged_env,
        text=True,
        encoding='utf-8',
        errors='replace',
        capture_output=True,
        check=False,
    )
    if stdout_file is not None and completed.stdout:
        stdout_file.write_text(completed.stdout, encoding='utf-8')
        stdout_text = f'[stdout written to {stdout_file.name}]\n'
    else:
        stdout_text = completed.stdout or ''
    log = '\n'.join([
        f'stage={name}',
        f'cwd={cwd or WORKSPACE}',
        f'command={json.dumps(args, ensure_ascii=False)}',
        f'returncode={completed.returncode}',
        '--- stdout ---',
        stdout_text,
        '--- stderr ---',
        completed.stderr or '',
    ])
    _write_log(f'{name}.log', log, secrets=secrets)
    return completed


def _require_success(completed: subprocess.CompletedProcess[str], stage: str) -> None:
    if completed.returncode != 0:
        raise PipelineError(stage)


def _load_payload() -> dict[str, str]:
    event = json.loads(EVENT_PATH.read_text(encoding='utf-8'))
    payload = event.get('client_payload') or {}
    private_repository = str(payload.get('private_repository') or '').strip()
    private_ref = str(payload.get('private_ref') or 'master').strip() or 'master'
    autoaction = str(payload.get('autoaction') or '1').strip()
    topic = str(payload.get('topic') or '')
    article_type = str(payload.get('article_type') or '养生食物型')
    additional_requirements = str(payload.get('additional_requirements') or '')
    upstream_run_id = str(payload.get('run_id') or '')

    if not private_repository or '/' not in private_repository:
        raise PipelineError('resolve-payload')
    try:
        count = int(autoaction)
    except ValueError as exc:
        raise PipelineError('resolve-payload') from exc
    if count < 1:
        raise PipelineError('resolve-payload')

    return {
        'private_repository': private_repository,
        'private_ref': private_ref,
        'autoaction': str(count),
        'topic': topic,
        'article_type': article_type,
        'additional_requirements': additional_requirements,
        'upstream_run_id': upstream_run_id,
    }


def _checkout_private(payload: dict[str, str], secrets: list[str]) -> None:
    token = os.environ.get('PRIVATE_REPO_TOKEN', '')
    if not token:
        raise PipelineError('checkout')
    print(f'::add-mask::{token}')
    print(f'::add-mask::{payload["private_repository"]}')
    repo_url = f'https://x-access-token:{token}@github.com/{payload["private_repository"]}.git'
    completed = _run(
        'checkout',
        ['git', 'clone', '--quiet', '--branch', payload['private_ref'], repo_url, str(PRIVATE_DIR)],
        secrets=secrets + [repo_url],
    )
    _require_success(completed, 'checkout')
    completed = _run('set-remote', ['git', 'remote', 'set-url', 'origin', repo_url], cwd=PRIVATE_DIR, secrets=secrets + [repo_url])
    _require_success(completed, 'set-remote')


def _copy_logs_to_private(payload: dict[str, str], status: str, failed_stage: str | None, secrets: list[str]) -> Path | None:
    if not PRIVATE_DIR.exists():
        return None
    timestamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    run_id = os.environ.get('GITHUB_RUN_ID', 'unknown')
    log_dir = PRIVATE_DIR / 'logs' / 'runner' / f'{timestamp}-{run_id}'
    log_dir.mkdir(parents=True, exist_ok=True)
    if TEMP_LOG_DIR.exists():
        for source in TEMP_LOG_DIR.glob('*.log'):
            shutil.copy2(source, log_dir / source.name)
    summary = {
        'status': status,
        'failed_stage': failed_stage,
        'runner_run_id': run_id,
        'upstream_run_id': payload.get('upstream_run_id'),
        'autoaction': payload.get('autoaction'),
        'private_ref': payload.get('private_ref'),
        'created_at_beijing': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='seconds'),
    }
    (log_dir / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return log_dir


def _commit_and_push(payload: dict[str, str], status: str, failed_stage: str | None, secrets: list[str]) -> None:
    log_dir = _copy_logs_to_private(payload, status, failed_stage, secrets)
    if log_dir is None:
        return

    _require_success(_run('git-config-name', ['git', 'config', 'user.name', 'github-actions[bot]'], cwd=PRIVATE_DIR, secrets=secrets), 'git-config-name')
    _require_success(_run('git-config-email', ['git', 'config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'], cwd=PRIVATE_DIR, secrets=secrets), 'git-config-email')
    add_paths = [path for path in ['articles', 'state/usage.json', 'site/data/articles.json', 'logs'] if (PRIVATE_DIR / path).exists()]
    if not add_paths:
        return
    _require_success(_run('git-add', ['git', 'add', *add_paths], cwd=PRIVATE_DIR, secrets=secrets), 'git-add')

    diff = _run('git-diff-cached', ['git', 'diff', '--cached', '--quiet'], cwd=PRIVATE_DIR, secrets=secrets)
    if diff.returncode == 0:
        return
    if diff.returncode != 1:
        raise PipelineError('git-diff-cached')

    message_status = 'completed' if status == 'success' else f'failed at {failed_stage}'
    _require_success(
        _run('git-commit', ['git', 'commit', '-m', f'chore: record runner pipeline {message_status}'], cwd=PRIVATE_DIR, secrets=secrets),
        'git-commit',
    )
    _require_success(
        _run('git-pull', ['git', 'pull', '--rebase', '--autostash', 'origin', payload['private_ref']], cwd=PRIVATE_DIR, secrets=secrets),
        'git-pull',
    )
    _require_success(
        _run('git-push', ['git', 'push', 'origin', f'HEAD:{payload["private_ref"]}'], cwd=PRIVATE_DIR, secrets=secrets),
        'git-push',
    )


def _run_pipeline(payload: dict[str, str], secrets: list[str]) -> None:
    env = {
        'CLAUDE_PROVIDERS_JSON': os.environ.get('CLAUDE_PROVIDERS_JSON', ''),
        'IMAGE_PROVIDERS_JSON': os.environ.get('IMAGE_PROVIDERS_JSON', ''),
        'PYTHONIOENCODING': 'utf-8',
        'TZ': 'Asia/Shanghai',
    }
    _require_success(_run('install-claude-cli', ['npm', 'install', '-g', '@anthropic-ai/claude-code'], secrets=secrets), 'install-claude-cli')
    _require_success(_run('claude-version', ['claude', '--version'], secrets=secrets), 'claude-version')
    _require_success(_run('uv-sync', ['uv', 'sync'], cwd=PRIVATE_DIR, secrets=secrets), 'uv-sync')
    _require_success(_run('validate-config', ['uv', 'run', 'python', 'scripts/validate_config.py'], cwd=PRIVATE_DIR, env=env, secrets=secrets), 'validate-config')

    count = int(payload['autoaction'])
    for index in range(1, count + 1):
        prompt_file = PRIVATE_DIR / f'claude_prompt_{index}.json'
        response_file = PRIVATE_DIR / f'claude_response_{index}.json'
        requirements = f'{payload["additional_requirements"]} 当前是今天自动批次第 {index} 篇，请尽量与今天前面的自动文章错开角度。'
        _require_success(
            _run(
                f'build-prompt-{index}',
                [
                    'uv', 'run', 'python', 'scripts/build_prompt.py',
                    '--topic', payload['topic'],
                    '--article-type', payload['article_type'],
                    '--additional-requirements', requirements,
                ],
                cwd=PRIVATE_DIR,
                env=env,
                secrets=secrets,
                stdout_file=prompt_file,
            ),
            f'build-prompt-{index}',
        )
        _require_success(
            _run(
                f'generate-article-{index}',
                ['uv', 'run', 'python', 'scripts/generate_article.py', '--prompt-file', prompt_file.name, '--output-file', response_file.name],
                cwd=PRIVATE_DIR,
                env=env,
                secrets=secrets,
            ),
            f'generate-article-{index}',
        )
        _require_success(
            _run(
                f'persist-article-{index}',
                ['uv', 'run', 'python', 'scripts/persist_article.py', '--response-file', response_file.name, '--article-type', payload['article_type']],
                cwd=PRIVATE_DIR,
                env=env,
                secrets=secrets,
            ),
            f'persist-article-{index}',
        )

    _require_success(_run('build-site-data', ['uv', 'run', 'python', 'scripts/build_site_data.py'], cwd=PRIVATE_DIR, env=env, secrets=secrets), 'build-site-data')


def main() -> int:
    TEMP_LOG_DIR.mkdir(parents=True, exist_ok=True)
    payload: dict[str, str] = {}
    failed_stage: str | None = None
    status = 'success'
    secrets = [
        os.environ.get('PRIVATE_REPO_TOKEN', ''),
        os.environ.get('CLAUDE_PROVIDERS_JSON', ''),
        os.environ.get('IMAGE_PROVIDERS_JSON', ''),
    ]
    try:
        print('[runner] start')
        payload = _load_payload()
        _write_log('payload-summary.log', json.dumps({
            'autoaction': payload['autoaction'],
            'private_ref': payload['private_ref'],
            'upstream_run_id': payload['upstream_run_id'],
            'topic': payload['topic'],
            'article_type': payload['article_type'],
            'additional_requirements': payload['additional_requirements'],
        }, ensure_ascii=False, indent=2), secrets=secrets)
        _checkout_private(payload, secrets)
        print('[runner] workspace ready')
        _run_pipeline(payload, secrets)
        print('[runner] pipeline completed')
    except PipelineError as exc:
        status = 'failure'
        failed_stage = str(exc)
        print(f'[runner] failed; stage={failed_stage}; private logs will be pushed when possible')
    except Exception as exc:
        status = 'failure'
        failed_stage = type(exc).__name__
        _write_log('unexpected-error.log', repr(exc), secrets=secrets)
        print('[runner] failed; private logs will be pushed when possible')
    try:
        if payload:
            _commit_and_push(payload, status, failed_stage, secrets)
            print('[runner] private logs pushed')
    except Exception as exc:
        _write_log('push-logs-error.log', repr(exc), secrets=secrets)
        print('[runner] failed to push private logs')
        return 1
    return 0 if status == 'success' else 1


if __name__ == '__main__':
    raise SystemExit(main())
