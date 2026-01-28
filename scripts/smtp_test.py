import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / "config" / ".env"
RECIPIENT = "mihailpavlov042006@gmail.com"


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def main() -> None:
    load_env(ENV_PATH)
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_from = os.getenv("SMTP_FROM", smtp_user or "no-reply@example.com")

    if not smtp_host or not smtp_user or not smtp_password:
        raise SystemExit("SMTP_* envs are missing. Check .env and how you run the script.")

    msg = EmailMessage()
    msg["Subject"] = "SMTP test письмо"
    msg["From"] = smtp_from
    msg["To"] = RECIPIENT
    msg.set_content("Тестовое письмо: SMTP работает.")

    if smtp_port == 465:
        server = smtplib.SMTP_SSL(smtp_host, smtp_port)
    else:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()

    with server:
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    print(f"Email sent to {RECIPIENT} from {smtp_from} via {smtp_host}:{smtp_port}")


if __name__ == "__main__":
    main()
