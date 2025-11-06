import argparse
import urllib
import subprocess
import re
import sys
import os

from bioblend.galaxy import GalaxyInstance


class APIError(Exception):
    pass


class GalaxyWrap:
    """A small wrapper around bioblend to make it easier to work with Galaxy in this script."""

    def __init__(self, url, key):
        self.url = url
        self.gi = GalaxyInstance(url, key)

    def make_get_request(self, endpoint, **kwargs):
        response = self.gi.make_get_request(
            urllib.parse.urljoin(self.url, endpoint), params=dict(**kwargs)
        )

        if response.status_code != 200:
            raise APIError(response.json()["err_msg"])

        return response.json()

    def get_jobs(self, **kwargs):
        return self.make_get_request("api/jobs", **kwargs)

    def get_job_by_id(self, job_id):
        return self.make_get_request(f"api/jobs/{job_id}")


def get_invocation_jobs(gw, invocation_id: str) -> list[dict]:
    jobs = gw.get_jobs(invocation_id=invocation_id)
    return [gw.get_job_by_id(job["id"]) for job in jobs]


def count_copied_invocation_jobs(gw:GalaxyWrap, invocation_id:str) -> dict:
    jobs = get_invocation_jobs(gw, invocation_id)
    return {"copied": len([job for job in jobs if job["copied_from_job_id"] is not None]), "total": len(jobs), "invocation_id": invocation_id}


def invocation_jobs_are_copied(
        gw: GalaxyWrap, invocation_id: str
) -> bool:
    """
    Validates that the invocation consists of copied jobs.
    """
    cnt = count_copied_invocation_jobs(gw, invocation_id)
    return cnt["copied"] == cnt["total"]




def run_planemo_and_get_invocation_id(command):
    """
    Runs a planemo command, captures its output, and extracts the invocation ID.

    Args:
        command: A list of strings representing the command to be executed.

    Returns:
        The invocation ID as a string, or None if it's not found.
    """

    try:
        # Create a copy of the current environment
        env = os.environ.copy()

        # Remove problematic environment variables that might leak the current
        # virtual environment into the subprocess
        env.pop('PYTHONPATH', None)
        # Optionally unset VIRTUAL_ENV if planemo still complains,
        # though PYTHONPATH is usually the main culprit.
        # env.pop('VIRTUAL_ENV', None)

        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            env=env  # Pass the sanitized environment
        )

        # The output from planemo often goes to stderr, so we check both stdout and stderr
        output = process.stdout + process.stderr

        # Use regular expression to find the invocation ID
        # This pattern looks for "Invocation <...>" and captures the part inside the angle brackets.
        match = re.search(r"Invocation <([^>]+)>", output)

        if match:
            invocation_id = match.group(1)
            return invocation_id
        else:
            print("Error: Invocation ID not found in the output.", file=sys.stderr)
            print("Full output:", output, file=sys.stderr)
            return None

    except FileNotFoundError:
        print(f"Error: The command '{command[0]}' was not found.", file=sys.stderr)
        print("Please ensure that 'uvx' and 'planemo' are in your system's PATH.", file=sys.stderr)
        return None
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}", file=sys.stderr)
        print(f"Return code: {e.returncode}", file=sys.stderr)
        print(f"Output:\n{e.stdout}{e.stderr}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return None


def run_workflow_and_check_cache(workflow_file: str, job_file: str, galaxy_url: str, galaxy_user_key: str) -> dict:
    """
    Run a Galaxy workflow via planemo, rerun it with cache, and verify that the rerun
    consists of copied jobs.

    This is a pure function (no argparse or sys.exit). It raises exceptions on errors
    and returns a result dictionary on success.

    Returns a dict with keys:
      - invocation_id: str
      - rerun_invocation_id: str
      - copied: int
      - total: int
      - success: bool  (True if all rerun jobs are copied and total > 0)
    """
    # Initial planemo run command
    planemo_command = [
        "planemo", "run",
        workflow_file,
        job_file,
        "--galaxy_url", galaxy_url,
        "--galaxy_user_key", galaxy_user_key,
    ]

    invocation_id = run_planemo_and_get_invocation_id(planemo_command)
    if not invocation_id:
        raise RuntimeError("Could not get invocation ID from the initial run.")

    # Planemo rerun command
    planemo_rerun_command = [
        "planemo", "rerun",
        "--use_cache",
        "--invocation", invocation_id,
        "--galaxy_url", galaxy_url,
        "--galaxy_user_key", galaxy_user_key,
    ]

    rerun_invocation_id = run_planemo_and_get_invocation_id(planemo_rerun_command)
    if not rerun_invocation_id:
        raise RuntimeError("Could not get invocation ID from the rerun.")

    # Check if the rerun jobs are copied
    gw = GalaxyWrap(galaxy_url, galaxy_user_key)
    count_data = count_copied_invocation_jobs(gw, rerun_invocation_id)
    success = count_data["copied"] == count_data["total"] and count_data["total"] > 0

    return {
        "invocation_id": invocation_id,
        "rerun_invocation_id": rerun_invocation_id,
        "copied": count_data["copied"],
        "total": count_data["total"],
        "success": success,
    }


def main():
    """
    CLI entry point: parse arguments and call the pure function.
    """
    parser = argparse.ArgumentParser(description="Run a Galaxy workflow and check for cached jobs on rerun.")
    parser.add_argument("workflow_file", help="Path to the workflow file (.ga)")
    parser.add_argument("job_file", help="Path to the job definition file (.yml)")
    parser.add_argument("--galaxy_url", default="http://127.0.0.1:8080/", help="URL of the Galaxy instance.")
    parser.add_argument("--galaxy_user_key", required=True, help="API key for the Galaxy user.")
    args = parser.parse_args()

    print("--- Running initial workflow ---")
    try:
        result = run_workflow_and_check_cache(
            workflow_file=args.workflow_file,
            job_file=args.job_file,
            galaxy_url=args.galaxy_url,
            galaxy_user_key=args.galaxy_user_key,
        )
    except APIError as e:
        print(f"An API error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    print(f"Successfully extracted Invocation ID: {result['invocation_id']}")
    print("\n--- Rerunning workflow from cache ---")
    print(f"Successfully extracted Rerun Invocation ID: {result['rerun_invocation_id']}")

    print("\n--- Verifying cached jobs ---")
    print(f"Copied jobs: {result['copied']} / {result['total']}")
    if result["success"]:
        print("Success: The rerun invocation consists of copied (cached) jobs.")
    else:
        print("Failure: The rerun invocation does not consist of entirely copied jobs.")


if __name__ == "__main__":
    main()
