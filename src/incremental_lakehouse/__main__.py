import argparse
import json
from pathlib import Path

from .experiments import evaluate


def main():
    parser = argparse.ArgumentParser(description="Typed incremental CDC warehouse and independent rebuild experiment")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("demo", "evaluate"):
        command = commands.add_parser(name)
        command.add_argument("--output")
        command.add_argument("--seed", type=int)
        command.add_argument("--config", help="JSON preset containing only an integer seed")
    args = parser.parse_args()
    config = {}
    if args.config:
        config_path = Path(args.config)
        if config_path.stat().st_size > 4096:
            parser.error("Configuration exceeds 4096 bytes")
        config = json.loads(config_path.read_text())
        if not isinstance(config, dict) or set(config) != {"seed"}:
            parser.error("Configuration requires exactly seed")
    report = evaluate(args.seed if args.seed is not None else config.get("seed", 17))
    if args.command == "demo":
        report.pop("cases")
    text = json.dumps(report, indent=2, allow_nan=False) + "\n"
    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    else:
        print(text, end="")
    if report["passed"] != report["checks"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
