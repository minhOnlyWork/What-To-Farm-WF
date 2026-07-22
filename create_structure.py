from pathlib import Path
import sys


FILES = [
    "README.md",
    "requirements.txt",
    ".env.example",
    ".gitignore",

    "app/__init__.py",
    "app/main.py",
    "app/pipeline.py",
    "app/market.py",
    "app/news.py",
    "app/model_a.py",
    "app/model_b.py",
    "app/features.py",
    "app/statistics.py",
    "app/storage.py",

    "frontend/index.html",
    "frontend/app.js",
    "frontend/style.css",

    "data/items.json",
    "data/latest_recommendations.json",
    "data/samples/market_sample.json",
    "data/samples/news_sample.json",

    "tests/__init__.py",
    "tests/test_statistics.py",
    "tests/test_model_a.py",
    "tests/test_api.py",

    ".github/workflows/test.yml",
    ".github/workflows/daily_pipeline.yml",
]


def create_project_structure(root: Path) -> None:
    created_count = 0
    skipped_count = 0

    for relative_path in FILES:
        file_path = root / relative_path

        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)

            if file_path.exists():
                print(f"[SKIPPED] {relative_path}")
                skipped_count += 1
                continue

            file_path.touch()
            print(f"[CREATED] {relative_path}")
            created_count += 1

        except OSError as error:
            print(f"[ERROR] Could not create {relative_path}: {error}")
            raise

    print()
    print("Project structure created successfully.")
    print(f"Created files: {created_count}")
    print(f"Existing files skipped: {skipped_count}")


def main() -> int:
    project_root = Path(__file__).resolve().parent

    try:
        create_project_structure(project_root)
        return 0
    except OSError:
        print("Project structure creation failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
