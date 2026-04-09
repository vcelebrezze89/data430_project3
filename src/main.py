import subprocess
import sys


def run_script(script_name):
    print(f"\nRunning {script_name}...")
    result = subprocess.run([sys.executable, script_name])

    if result.returncode != 0:
        print(f"Error running {script_name}")
        sys.exit(1)


def main():
    print("Starting Quotes Data Pipeline")

    run_script("src/scraper.py")
    run_script("src/processor.py")
    run_script("src/database.py")

    print("\nPipeline completed successfully!")


if __name__ == "__main__":
    main()