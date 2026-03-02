from bs4 import BeautifulSoup
from datetime import datetime, timedelta

def parse_activity_page(html_content: str) -> dict:
    soup = BeautifulSoup(html_content, 'html.parser')
    activity_items = soup.find_all('li', class_='event')

    activity_summary = {
        'events': [],
        'narrative': []
    }

    for item in activity_items:
        event_type = item.find('span', class_='event-icon').get('title')
        event_time = item.find('span', class_='event-time').text.strip()
        event_description = item.find('span', class_='event-title').text.strip()

        activity_summary['events'].append({
            'type': event_type,
            'time': event_time,
            'description': event_description
        })

    activity_summary['narrative'] = _generate_narrative(activity_summary['events'])

    return activity_summary

def _generate_narrative(events: list) -> list:
    narrative = []
    for event in events:
        narrative.append(f"- {event['type']} at {event['time']}: {event['description']}")
    return narrative
