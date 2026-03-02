import os
import requests
from pathlib import Path
from datetime import datetime, timedelta
import json

from typing import Optional

class GitLabClient:
    def __init__(self):
        self.base_url = "https://eegitlab.fit.nasa.gov"
        self.token = os.getenv("GITLAB_TOKEN")
        self.headers = {'PRIVATE-TOKEN': self.token} if self.token else {}
        self.monitored_projects = self._load_monitored_projects()

    def _load_monitored_projects(self) -> dict:
        monitored_path = Path('./data/monitored.json')
        if monitored_path.exists():
            with open(monitored_path, 'r') as f:
                return json.load(f)
        return {}

    def get_group_project_stats(self, group_name: str, days: int) -> dict:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        projects = self._get_group_projects(group_name)
        project_stats = {}

        for project in projects:
            if project['id'] not in self.monitored_projects:
                continue
            project_id = project['id']
            project_name = project['name']
            project_stats[project_name] = {
                'open_mrs': self._get_open_mrs(project_id, start_date, end_date),
                'merged_mrs': self._get_merged_mrs(project_id, start_date, end_date),
                'active_branches': self._get_active_branches(project_id, start_date, end_date)
            }

        return project_stats

    def _get_group_projects(self, group_name: str) -> list:
        response = requests.get(f"{self.base_url}/api/v4/groups/{group_name}/projects", headers=self.headers)
        response.raise_for_status()
        return response.json()

    def _get_open_mrs(self, project_id: int, start_date: datetime, end_date: datetime) -> list:
        response = requests.get(f"{self.base_url}/api/v4/projects/{project_id}/merge_requests?state=opened", headers=self.headers)
        response.raise_for_status()
        return [mr for mr in response.json() if start_date <= datetime.fromisoformat(mr['created_at'][:-1]) <= end_date]

    def _get_merged_mrs(self, project_id: int, start_date: datetime, end_date: datetime) -> list:
        response = requests.get(f"{self.base_url}/api/v4/projects/{project_id}/merge_requests?state=merged", headers=self.headers)
        response.raise_for_status()
        return [mr for mr in response.json() if start_date <= datetime.fromisoformat(mr['merged_at'][:-1]) <= end_date]

    def _get_active_branches(self, project_id: int, start_date: datetime, end_date: datetime) -> list:
        response = requests.get(f"{self.base_url}/api/v4/projects/{project_id}/repository/branches", headers=self.headers)
        response.raise_for_status()
        return [branch for branch in response.json() if self._branch_has_recent_commits(project_id, branch['name'], start_date, end_date)]

    def get_diff(self, project_id: int, mr_iid: Optional[int] = None, branch_name: Optional[str] = None) -> str:
        if mr_iid:
            response = requests.get(f"{self.base_url}/api/v4/projects/{project_id}/merge_requests/{mr_iid}/diffs", headers=self.headers)
        elif branch_name:
            response = requests.get(f"{self.base_url}/api/v4/projects/{project_id}/repository/compare?from={branch_name}&to=main", headers=self.headers)
        else:
            raise ValueError("Either mr_iid or branch_name must be provided")
        response.raise_for_status()
        return response.text

    def _branch_has_recent_commits(self, project_id: int, branch_name: str, start_date: datetime, end_date: datetime) -> bool:
        response = requests.get(f"{self.base_url}/api/v4/projects/{project_id}/repository/commits?ref_name={branch_name}", headers=self.headers)
        response.raise_for_status()
        return any(start_date <= datetime.fromisoformat(commit['authored_date'][:-1]) <= end_date for commit in response.json())
