from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
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


def _write_json_log(name: str, payload: dict[str, object], *, secrets: list[str]) -> None:
    _write_log(name, json.dumps(payload, ensure_ascii=False, indent=2, default=str), secrets=secrets)


def _now_beijing() -> str:
    return datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='seconds')


def _provider_summary(raw: str) -> list[dict[str, object]]:
    try:
        providers = json.loads(raw or '[]')
    except json.JSONDecodeError:
        return [{'error': 'invalid provider json'}]
    if not isinstance(providers, list):
        return [{'error': 'provider json is not a list'}]
    summary: list[dict[str, object]] = []
    for provider in providers:
        if not isinstance(provider, dict):
            continue
        summary.append({
            'name': provider.get('name'),
            'type': provider.get('type'),
            'model': provider.get('model'),
            'priority': provider.get('priority'),
            'enabled': provider.get('enabled'),
            'timeout_seconds': provider.get('timeout_seconds'),
            'max_turns': provider.get('max_turns'),
        })
    return summary


def _file_info(path: Path) -> dict[str, object]:
    if not path.exists():
        return {'path': str(path), 'exists': False}
    return {'path': str(path), 'exists': True, 'size_bytes': path.stat().st_size}


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
    started_at = _now_beijing()
    started = time.monotonic()
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
    duration_seconds = round(time.monotonic() - started, 3)
    completed_at = _now_beijing()
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
        f'started_at_beijing={started_at}',
        f'completed_at_beijing={completed_at}',
        f'duration_seconds={duration_seconds}',
        f'env_override_keys={json.dumps(sorted((env or {}).keys()), ensure_ascii=False)}',
        f'stdout_chars={len(completed.stdout or "")}',
        f'stderr_chars={len(completed.stderr or "")}',
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
    content_skill = str(payload.get('content_skill') or '').strip()
    raw_runs_config = payload.get('content_skill_runs_json')
    if isinstance(raw_runs_config, str):
        content_skill_runs_json = raw_runs_config.strip()
    elif raw_runs_config:
        content_skill_runs_json = json.dumps(raw_runs_config, ensure_ascii=False)
    else:
        content_skill_runs_json = ''
    iteration = str(payload.get('iteration') or '').strip()
    runs = str(payload.get('runs') or '').strip()
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
        'content_skill': content_skill,
        'content_skill_runs_json': content_skill_runs_json,
        'iteration': iteration,
        'runs': runs,
        'upstream_run_id': upstream_run_id,
    }


def _build_run_plan(payload: dict[str, str]) -> list[dict[str, str]]:
    raw_config = payload.get('content_skill_runs_json', '').strip()
    if raw_config:
        try:
            runs_config = json.loads(raw_config)
        except json.JSONDecodeError as exc:
            raise PipelineError('resolve-run-plan') from exc
        if isinstance(runs_config, dict):
            runs_config = [runs_config]
        if not isinstance(runs_config, list) or not runs_config:
            raise PipelineError('resolve-run-plan')

        run_plan: list[dict[str, str]] = []
        for index, item in enumerate(runs_config, 1):
            if not isinstance(item, dict):
                raise PipelineError('resolve-run-plan')
            content_skill = str(item.get('skill') or item.get('content_skill') or '').strip()
            if not content_skill:
                raise PipelineError('resolve-run-plan')
            try:
                runs = int(item.get('runs', 1))
            except (TypeError, ValueError) as exc:
                raise PipelineError('resolve-run-plan') from exc
            if runs < 1:
                raise PipelineError('resolve-run-plan')
            for iteration in range(1, runs + 1):
                run_plan.append({'content_skill': content_skill, 'iteration': str(iteration), 'runs': str(runs)})
        return run_plan

    count = int(payload['autoaction'])
    return [
        {'content_skill': payload.get('content_skill', ''), 'iteration': str(index), 'runs': str(count)}
        for index in range(1, count + 1)
    ]


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
    _write_checkout_summary(payload, secrets)


def _git_output(args: list[str], *, cwd: Path) -> str:
    completed = subprocess.run(args, cwd=cwd, text=True, encoding='utf-8', errors='replace', capture_output=True, check=False)
    return completed.stdout.strip() if completed.returncode == 0 else ''


def _write_checkout_summary(payload: dict[str, str], secrets: list[str]) -> None:
    _write_json_log('checkout-summary.log', {
        'private_repository': payload.get('private_repository'),
        'private_ref': payload.get('private_ref'),
        'head_sha': _git_output(['git', 'rev-parse', 'HEAD'], cwd=PRIVATE_DIR),
        'head_subject': _git_output(['git', 'log', '-1', '--format=%s'], cwd=PRIVATE_DIR),
        'tracked_files': len(_git_output(['git', 'ls-files'], cwd=PRIVATE_DIR).splitlines()),
        'created_at_beijing': _now_beijing(),
    }, secrets=secrets)


def _write_environment_summary(payload: dict[str, str], secrets: list[str]) -> None:
    _write_json_log('environment-summary.log', {
        'runner_run_id': os.environ.get('GITHUB_RUN_ID'),
        'runner_repository': os.environ.get('GITHUB_REPOSITORY'),
        'upstream_run_id': payload.get('upstream_run_id'),
        'autoaction': payload.get('autoaction'),
        'article_type': payload.get('article_type'),
        'content_skill': payload.get('content_skill'),
        'content_skill_runs_json': payload.get('content_skill_runs_json'),
        'run_plan': _build_run_plan(payload),
        'iteration': payload.get('iteration'),
        'runs': payload.get('runs'),
        'pythonioencoding': os.environ.get('PYTHONIOENCODING'),
        'timezone': os.environ.get('TZ'),
        'claude_providers': _provider_summary(os.environ.get('CLAUDE_PROVIDERS_JSON', '')),
        'image_providers': _provider_summary(os.environ.get('IMAGE_PROVIDERS_JSON', '')),
        'created_at_beijing': _now_beijing(),
    }, secrets=secrets)


def _write_prompt_summary(index: int, prompt_file: Path, secrets: list[str]) -> None:
    payload: dict[str, object] = {'index': index, 'prompt_file': _file_info(prompt_file), 'created_at_beijing': _now_beijing()}
    try:
        prompt_payload = json.loads(prompt_file.read_text(encoding='utf-8'))
        prompt_text = str(prompt_payload.get('prompt') or '') if isinstance(prompt_payload, dict) else ''
        payload.update({
            'topic': prompt_payload.get('topic') if isinstance(prompt_payload, dict) else None,
            'article_type': prompt_payload.get('article_type') if isinstance(prompt_payload, dict) else None,
            'additional_requirements': prompt_payload.get('additional_requirements') if isinstance(prompt_payload, dict) else None,
            'warnings': prompt_payload.get('warnings') if isinstance(prompt_payload, dict) else None,
            'prompt_chars': len(prompt_text),
            'has_health_food_skill': 'skills/HealthFoodSkill' in prompt_text,
            'has_health_food_list_skill': 'skills/HealthFoodListSkill' in prompt_text,
            'has_article_skill_context': '## skills/ArticleSkill/' in prompt_text,
            'has_image_planning': '配图规划' in prompt_text and 'image_slots' in prompt_text,
        })
    except Exception as exc:
        payload['error'] = repr(exc)
    _write_json_log(f'prompt-summary-{index}.log', payload, secrets=secrets)


def _write_response_summary(index: int, response_file: Path, secrets: list[str]) -> None:
    payload: dict[str, object] = {'index': index, 'response_file': _file_info(response_file), 'created_at_beijing': _now_beijing()}
    try:
        response = json.loads(response_file.read_text(encoding='utf-8'))
        image_slots = response.get('image_slots', []) if isinstance(response, dict) else []
        payload.update({
            'title': response.get('title') if isinstance(response, dict) else None,
            'article_type': response.get('article_type') if isinstance(response, dict) else None,
            'summary': response.get('summary') if isinstance(response, dict) else None,
            'word_count': response.get('word_count') if isinstance(response, dict) else None,
            'hot_topics': response.get('hot_topics') if isinstance(response, dict) else None,
            'reference_count': len(response.get('references', [])) if isinstance(response.get('references', []), list) else None,
            'image_slot_count': len(image_slots) if isinstance(image_slots, list) else None,
            'image_slot_alts': [slot.get('alt') for slot in image_slots if isinstance(slot, dict)] if isinstance(image_slots, list) else None,
            'markdown_chars': len(str(response.get('markdown_body') or '')) if isinstance(response, dict) else None,
        })
    except Exception as exc:
        payload['error'] = repr(exc)
    _write_json_log(f'response-summary-{index}.log', payload, secrets=secrets)


def _write_private_status_summary(name: str, secrets: list[str]) -> None:
    _write_json_log(f'{name}.log', {
        'head_sha': _git_output(['git', 'rev-parse', 'HEAD'], cwd=PRIVATE_DIR),
        'status_short': _git_output(['git', 'status', '--short'], cwd=PRIVATE_DIR).splitlines(),
        'latest_articles': _git_output(['git', 'ls-files', 'articles'], cwd=PRIVATE_DIR).splitlines()[-20:],
        'usage_file': _file_info(PRIVATE_DIR / 'state' / 'usage.json'),
        'site_data_file': _file_info(PRIVATE_DIR / 'site' / 'data' / 'articles.json'),
        'created_at_beijing': _now_beijing(),
    }, secrets=secrets)


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
    log_files = sorted(path.name for path in log_dir.glob('*.log'))
    summary = {
        'status': status,
        'failed_stage': failed_stage,
        'runner_run_id': run_id,
        'upstream_run_id': payload.get('upstream_run_id'),
        'autoaction': payload.get('autoaction'),
        'private_ref': payload.get('private_ref'),
        'log_file_count': len(log_files),
        'log_files': log_files,
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
    _write_environment_summary(payload, secrets)
    _require_success(_run('install-claude-cli', ['npm', 'install', '-g', '@anthropic-ai/claude-code'], secrets=secrets), 'install-claude-cli')
    _require_success(_run('claude-version', ['claude', '--version'], secrets=secrets), 'claude-version')
    _require_success(_run('uv-sync', ['uv', 'sync'], cwd=PRIVATE_DIR, secrets=secrets), 'uv-sync')
    _require_success(_run('validate-config', ['uv', 'run', 'python', 'scripts/validate_config.py'], cwd=PRIVATE_DIR, env=env, secrets=secrets), 'validate-config')

    run_plan = _build_run_plan(payload)
    for index, item in enumerate(run_plan, 1):
        prompt_file = PRIVATE_DIR / f'claude_prompt_{index}.json'
        response_file = PRIVATE_DIR / f'claude_response_{index}.json'
        content_skill = item['content_skill']
        content_skill_label = content_skill or '默认内容包'
        requirements = f'{payload["additional_requirements"]} 当前是内容包 {content_skill_label} 的自动批次第 {item["iteration"]}/{item["runs"]} 篇，请尽量与同内容包今天前面的自动文章错开角度。'
        build_prompt_args = [
            'uv', 'run', 'python', 'scripts/build_prompt.py',
            '--topic', payload['topic'],
            '--article-type', payload['article_type'],
            '--additional-requirements', requirements,
        ]
        if content_skill:
            build_prompt_args.extend(['--content-skill', content_skill])
        _require_success(
            _run(
                f'build-prompt-{index}',
                build_prompt_args,
                cwd=PRIVATE_DIR,
                env=env,
                secrets=secrets,
                stdout_file=prompt_file,
            ),
            f'build-prompt-{index}',
        )
        _write_prompt_summary(index, prompt_file, secrets)
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
        _write_response_summary(index, response_file, secrets)
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
        _require_success(
            _run(
                f'build-site-data-{index}',
                ['uv', 'run', 'python', 'scripts/build_site_data.py'],
                cwd=PRIVATE_DIR,
                env=env,
                secrets=secrets,
            ),
            f'build-site-data-{index}',
        )

    _write_private_status_summary('private-status-after-pipeline', secrets)


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
            'content_skill': payload['content_skill'],
            'content_skill_runs_json': payload['content_skill_runs_json'],
            'run_plan': _build_run_plan(payload),
            'iteration': payload['iteration'],
            'runs': payload['runs'],
        }, ensure_ascii=False, indent=2), secrets=secrets)
        _checkout_private(payload, secrets)
        print('[runner] workspace ready')
        _run_pipeline(payload, secrets)
        print('[runner] pipeline completed')
    except PipelineError as exc:
        status = 'failure'
        failed_stage = str(exc)
        if PRIVATE_DIR.exists():
            _write_private_status_summary('private-status-after-failure', secrets)
        print(f'[runner] failed; stage={failed_stage}; private logs will be pushed when possible')
    except Exception as exc:
        status = 'failure'
        failed_stage = type(exc).__name__
        _write_log('unexpected-error.log', repr(exc), secrets=secrets)
        if PRIVATE_DIR.exists():
            _write_private_status_summary('private-status-after-failure', secrets)
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
