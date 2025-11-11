import requests
import json
import time
import sys
from typing import Dict, Any

# Configuration
KIBANA_URL = "http://localhost:5601"
ES_URL = "http://localhost:9200"


class KibanaVisualization:
    def __init__(self, kibana_url=KIBANA_URL, es_url=ES_URL):
        self.kibana_url = kibana_url
        self.es_url = es_url
        self.headers = {
            "kbn-xsrf": "true",
            "Content-Type": "application/json"
        }
        self.created_objects = []
    
    def create_or_update(self, object_type, object_id, payload:dict):
        url = f"{self.kibana_url}/a[i/saved_objects/{object_type}/{object_id}"
        
        try:
            response = requests.post(
                url=url,
                headers=self.headers,
                json=payload,
                params={"overwrite": "true"}
            )
            if response.ok:
                self.created_objects.append({
                    "type": object_type,
                    "id": object_id,
                    "title": payload.get("attributes", {}).get('title', object_id)
                })
                return True
            else:
                print(f"{object_type}/{object_id}: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"Error: {e}")
            return False
    
    def create_index_pattern(self):
        patterns = [
            {
                "id": "reddit_submissions",
                "title": "reddit_submissions*",
                "timeFieldName": "emitted_at"
            },
            {
                "id": "reddit_comments",
                "title": "reddit_comments*",
                "timeFieldName": "emitted_at"
            }
        ]

        for pattern in patterns:
            payload = {
                "attributes": {
                    "title": pattern["title"],
                    "timeFieldName": pattern["timeFieldName"]
                }
            }
            
            if self.create_or_update("index-pattern", pattern["id"], payload):
                print(f"{pattern['title']}")
    
    def create_visualizations(self):
        """Create all visualizations."""
        viz_posts_timeline = {
            "attributes": {
                "title": "Reddit Posts Over Time",
                "visState": json.dumps({
                    "title": "Reddit Posts Over Time",
                    "type": "line",
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "params": {},
                            "schema": "metric"
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "date_histogram",
                            "params": {
                                "field": "payload.created_utc",
                                "timeRange": {"from": "now-1h", "to": "now"},
                                "useNormalizedEsInterval": True,
                                "scaleMetricValues": False,
                                "interval": "auto",
                                "drop_partials": False,
                                "min_doc_count": 1,
                                "extended_bounds": {}
                            },
                            "schema": "segment"
                        }
                    ],
                    "params": {
                        "type": "line",
                        "grid": {"categoryLines": False},
                        "categoryAxes": [{
                            "id": "CategoryAxis-1",
                            "type": "category",
                            "position": "bottom",
                            "show": True,
                            "title": {}
                        }],
                        "valueAxes": [{
                            "id": "ValueAxis-1",
                            "name": "LeftAxis-1",
                            "type": "value",
                            "position": "left",
                            "show": True,
                            "title": {"text": "Count"}
                        }],
                        "seriesParams": [{
                            "show": True,
                            "type": "line",
                            "mode": "normal",
                            "data": {"label": "Count", "id": "1"},
                            "valueAxis": "ValueAxis-1",
                            "drawLinesBetweenPoints": True,
                            "lineWidth": 2,
                            "showCircles": True
                        }],
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "times": [],
                        "addTimeMarker": False,
                        "thresholdLine": {"show": False}
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Timeline showing Reddit post volume",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": "reddit_submissions",
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            }
        }
        self.create_or_update("visualization", "reddit-posts-timeline", viz_posts_timeline)
        print("Posts Over Time")
        
        viz_top_subreddits = {
            "attributes": {
                "title": "Top 10 Subreddits",
                "visState": json.dumps({
                    "title": "Top 10 Subreddits",
                    "type": "pie",
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "params": {},
                            "schema": "metric"
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "terms",
                            "params": {
                                "field": "payload.subreddit.keyword",
                                "orderBy": "1",
                                "order": "desc",
                                "size": 10,
                                "otherBucket": False,
                                "otherBucketLabel": "Other",
                                "missingBucket": False,
                                "missingBucketLabel": "Missing"
                            },
                            "schema": "segment"
                        }
                    ],
                    "params": {
                        "type": "pie",
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "isDonut": False,
                        "labels": {
                            "show": True,
                            "values": True,
                            "last_level": True,
                            "truncate": 100
                        }
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Distribution of posts across top subreddits",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": "reddit_submissions",
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            }
        }
        self.create_or_update("visualization", "reddit-top-subreddits", viz_top_subreddits)
        print("Top Subreddits")
        
        viz_total_posts = {
            "attributes": {
                "title": "Total Posts",
                "visState": json.dumps({
                    "title": "Total Posts",
                    "type": "metric",
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "params": {},
                            "schema": "metric"
                        }
                    ],
                    "params": {
                        "addTooltip": True,
                        "addLegend": False,
                        "type": "metric",
                        "metric": {
                            "percentageMode": False,
                            "useRanges": False,
                            "colorSchema": "Green to Red",
                            "metricColorMode": "None",
                            "colorsRange": [{"from": 0, "to": 10000}],
                            "labels": {"show": True},
                            "invertColors": False,
                            "style": {
                                "bgFill": "#000",
                                "bgColor": False,
                                "labelColor": False,
                                "subText": "",
                                "fontSize": 60
                            }
                        }
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Total number of Reddit posts",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": "reddit_submissions",
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            }
        }
        self.create_or_update("visualization", "reddit-total-posts", viz_total_posts)
        print("Total Posts")
        
        viz_avg_score = {
            "attributes": {
                "title": "Average Post Score",
                "visState": json.dumps({
                    "title": "Average Post Score",
                    "type": "metric",
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "avg",
                            "params": {"field": "payload.score"},
                            "schema": "metric"
                        }
                    ],
                    "params": {
                        "addTooltip": True,
                        "addLegend": False,
                        "type": "metric",
                        "metric": {
                            "percentageMode": False,
                            "useRanges": False,
                            "colorSchema": "Green to Red",
                            "metricColorMode": "None",
                            "colorsRange": [{"from": 0, "to": 1000}],
                            "labels": {"show": True},
                            "invertColors": False,
                            "style": {
                                "bgFill": "#000",
                                "bgColor": False,
                                "labelColor": False,
                                "subText": "",
                                "fontSize": 60
                            }
                        }
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Average score (upvotes) of posts",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": "reddit_submissions",
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            }
        }
        self.create_or_update("visualization", "reddit-avg-score", viz_avg_score)
        print("Average Score")
        
        viz_keywords = {
            "attributes": {
                "title": "Top Keywords",
                "visState": json.dumps({
                    "title": "Top Keywords",
                    "type": "tagcloud",
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "params": {},
                            "schema": "metric"
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "terms",
                            "params": {
                                "field": "keywords",
                                "orderBy": "1",
                                "order": "desc",
                                "size": 50,
                                "otherBucket": False,
                                "missingBucket": False
                            },
                            "schema": "segment"
                        }
                    ],
                    "params": {
                        "scale": "linear",
                        "orientation": "single",
                        "minFontSize": 18,
                        "maxFontSize": 72,
                        "showLabel": True
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Most common keywords extracted from posts",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": "reddit_submissions",
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            }
        }
        self.create_or_update("visualization", "reddit-keywords", viz_keywords)
        print("Top Keywords")
        
        viz_heatmap = {
            "attributes": {
                "title": "Posts by Hour of Day",
                "visState": json.dumps({
                    "title": "Posts by Hour of Day",
                    "type": "heatmap",
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "params": {},
                            "schema": "metric"
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "date_histogram",
                            "params": {
                                "field": "payload.created_utc",
                                "timeRange": {"from": "now-7d", "to": "now"},
                                "useNormalizedEsInterval": True,
                                "interval": "h",
                                "drop_partials": False,
                                "min_doc_count": 0
                            },
                            "schema": "segment"
                        }
                    ],
                    "params": {
                        "type": "heatmap",
                        "addTooltip": True,
                        "addLegend": True,
                        "enableHover": False,
                        "legendPosition": "right",
                        "times": [],
                        "colorsNumber": 4,
                        "colorSchema": "Blues",
                        "setColorRange": False,
                        "colorsRange": [],
                        "invertColors": False,
                        "percentageMode": False,
                        "valueAxes": [{
                            "show": False,
                            "id": "ValueAxis-1",
                            "type": "value",
                            "scale": {"type": "linear", "defaultYExtents": False},
                            "labels": {"show": False, "rotate": 0, "color": "#555"}
                        }]
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Heatmap showing post activity by hour",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": "reddit_submissions",
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            }
        }
        self.create_or_update("visualization", "reddit-heatmap", viz_heatmap)
        print("Posts by Hour")

    
    def create_dashboard(self):
        panels = [
            {
                "version": "8.13.4",
                "gridData": {"x": 0, "y": 0, "w": 24, "h": 8, "i": "1"},
                "panelIndex": "1",
                "embeddableConfig": {},
                "panelRefName": "panel_1"
            },
            {
                "version": "8.13.4",
                "gridData": {"x": 24, "y": 0, "w": 24, "h": 8, "i": "2"},
                "panelIndex": "2",
                "embeddableConfig": {},
                "panelRefName": "panel_2"
            },
            {
                "version": "8.13.4",
                "gridData": {"x": 0, "y": 8, "w": 12, "h": 6, "i": "3"},
                "panelIndex": "3",
                "embeddableConfig": {},
                "panelRefName": "panel_3"
            },
            {
                "version": "8.13.4",
                "gridData": {"x": 12, "y": 8, "w": 12, "h": 6, "i": "4"},
                "panelIndex": "4",
                "embeddableConfig": {},
                "panelRefName": "panel_4"
            },
            {
                "version": "8.13.4",
                "gridData": {"x": 24, "y": 8, "w": 24, "h": 14, "i": "5"},
                "panelIndex": "5",
                "embeddableConfig": {},
                "panelRefName": "panel_5"
            },
            {
                "version": "8.13.4",
                "gridData": {"x": 0, "y": 14, "w": 24, "h": 8, "i": "6"},
                "panelIndex": "6",
                "embeddableConfig": {},
                "panelRefName": "panel_6"
            }
        ]
        
        references = [
            {
                "name": "panel_1",
                "type": "visualization",
                "id": "reddit-posts-timeline"
            },
            {
                "name": "panel_2",
                "type": "visualization",
                "id": "reddit-top-subreddits"
            },
            {
                "name": "panel_3",
                "type": "visualization",
                "id": "reddit-total-posts"
            },
            {
                "name": "panel_4",
                "type": "visualization",
                "id": "reddit-avg-score"
            },
            {
                "name": "panel_5",
                "type": "visualization",
                "id": "reddit-keywords"
            },
            {
                "name": "panel_6",
                "type": "visualization",
                "id": "reddit-heatmap"
            }
        ]
        
        dashboard = {
            "attributes": {
                "title": "Reddit Analytics Dashboard",
                "description": "Real-time Reddit post analytics",
                "panelsJSON": json.dumps(panels),
                "optionsJSON": json.dumps({
                    "useMargins": True,
                    "hidePanelTitles": False
                }),
                "version": 1,
                "timeRestore": True,
                "timeTo": "now",
                "timeFrom": "now-1h",
                "refreshInterval": {
                    "pause": False,
                    "value": 5000 
                },
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            },
            "references": references
        }
        
        if self.create_or_update("dashboard", "reddit-analytics", dashboard):
            print("Reddit Analytics Dashboard")
            return True
        return False
    

if __name__ == "__main__":
    setup = KibanaVisualization()
    setup.create_index_pattern()
    time.sleep(2)
    setup.create_visualizations()
    time.sleep(2)
    setup.create_dashboard()
