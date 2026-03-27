from __future__ import annotations

import argparse
import os
import posixpath
from pathlib import Path

import paramiko


LOCAL_SCRIPT = Path(__file__).with_name("tools") / "docatlas_telegram_bot.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy the repo-managed DocAtlas Telegram bot to the server.")
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--token", default=os.environ.get("DOCATLAS_TELEGRAM_BOT_TOKEN"))
    parser.add_argument("--chat-id", default=os.environ.get("DOCATLAS_TELEGRAM_CHAT_ID"))
    parser.add_argument("--remote-repo", default="/home/faisal/DocAtlas")
    parser.add_argument("--docatlas-root", default="/mnt/nas/faisal/DocAtlas")
    parser.add_argument("--remote-log", default="/tmp/docatlas_status_bot.log")
    parser.add_argument("--tmux-session", default="docatlas_status_bot")
    return parser.parse_args()


def ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    parts = [part for part in remote_dir.split("/") if part]
    current = ""
    for part in parts:
        current += "/" + part
        try:
            sftp.stat(current)
        except IOError:
            sftp.mkdir(current)


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def main() -> int:
    args = parse_args()
    if not args.token or not args.chat_id:
        raise SystemExit("Both --token and --chat-id are required.")
    if not LOCAL_SCRIPT.exists():
        raise SystemExit(f"Missing local bot script: {LOCAL_SCRIPT}")

    remote_script = posixpath.join(args.remote_repo, "tools", "docatlas_telegram_bot.py")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(args.host, username=args.user, password=args.password, timeout=20)
    try:
        sftp = client.open_sftp()
        try:
            ensure_remote_dir(sftp, posixpath.dirname(remote_script))
            sftp.put(str(LOCAL_SCRIPT), remote_script)
            sftp.chmod(remote_script, 0o775)
        finally:
            sftp.close()

        quoted_command = (
            f"python3 {shell_quote(remote_script)} "
            f"--token {shell_quote(args.token)} "
            f"--chat-id {shell_quote(str(args.chat_id))} "
            f"--docatlas-root {shell_quote(args.docatlas_root)} "
            f">>{shell_quote(args.remote_log)} 2>&1"
        )
        command = (
            "pkill -f 'tools/docatlas_telegram_bot.py' || true; "
            "pkill -f '/tmp/docatlas_status_bot.py' || true; "
            f"tmux kill-session -t {shell_quote(args.tmux_session)} 2>/dev/null || true; "
            f"tmux new-session -d -s {shell_quote(args.tmux_session)} {shell_quote('bash -lc ' + shell_quote(quoted_command))}; "
            "sleep 2; "
            f"tmux ls | grep {shell_quote(args.tmux_session)} || true"
        )
        stdin, stdout, stderr = client.exec_command(command)
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        print(out.strip())
        if err.strip():
            print(err.strip())
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
