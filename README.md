# GroupStats

## Setup

1. Install dependencies:
   ```bash
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Set up environment variables (optional, for GitLab API access):
   ```bash
   echo "GITLAB_TOKEN=your_token_here" > .env
   ```

3. Run the tool:
   ```bash
   python generate_summary.py --group emss --days 7 --out ./out
   ```

## Testing

1. Run the GitLab API test script:
   ```bash
   python scripts/test_gitlab_api.py
   ```

## Output

- The tool generates `./out/weekly_summary.md`.
- The test script generates `./out/gitlab_api_test.md`.
