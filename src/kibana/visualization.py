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
        url = f"{self.kibana_url}/api/saved_objects/{object_type}/{object_id}"
        
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
        
        # ========== ORIGINAL 6 VISUALIZATIONS ==========
        print("\n=== Original Visualizations ===")
        
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
        print("✓ Posts Over Time")
        
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
                                "field": "payload.subreddit",
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
        print("✓ Top Subreddits")
        
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
        print("✓ Total Posts")
        
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
        print("✓ Average Score")
        
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
        print("✓ Top Keywords")
        
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
        print("✓ Posts by Hour")

        # ========== NEW VISUALIZATIONS ==========
        print("\n=== New Enhanced Visualizations ===")
        
        # 1. Score Distribution Histogram
        viz_score_dist = {
            "attributes": {
                "title": "Score Distribution",
                "visState": json.dumps({
                    "title": "Score Distribution",
                    "type": "histogram",
                    "aggs": [
                        {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
                        {"id": "2", "enabled": True, "type": "histogram", "params": {
                            "field": "payload.score", "interval": 5, "min_doc_count": 1
                        }, "schema": "segment"}
                    ],
                    "params": {
                        "type": "histogram",
                        "grid": {"categoryLines": False},
                        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom", 
                                        "show": True, "title": {"text": "Score Range"}}],
                        "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", 
                                      "position": "left", "show": True, "title": {"text": "Count"}}],
                        "seriesParams": [{"show": True, "type": "histogram", "mode": "normal", 
                                        "data": {"label": "Count", "id": "1"}, "valueAxis": "ValueAxis-1"}],
                        "addTooltip": True, "addLegend": False
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Distribution of post scores",
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                    "index": "reddit_submissions", "query": {"query": "", "language": "kuery"}, "filter": []})}
            }
        }
        self.create_or_update("visualization", "reddit-score-dist", viz_score_dist)
        print("✓ Score Distribution Histogram")
        
        # 2. Comments vs Score Scatter (using Data Table as alternative)
        viz_comments_vs_score = {
            "attributes": {
                "title": "Top Posts: Comments vs Score",
                "visState": json.dumps({
                    "title": "Top Posts: Comments vs Score",
                    "type": "table",
                    "aggs": [
                        {"id": "1", "enabled": True, "type": "top_hits", "params": {
                            "field": "payload.title", "aggregate": "concat", "size": 20,
                            "sortField": "payload.score", "sortOrder": "desc"
                        }, "schema": "metric"},
                        {"id": "2", "enabled": True, "type": "top_hits", "params": {
                            "field": "payload.score", "aggregate": "max", "size": 1
                        }, "schema": "metric"},
                        {"id": "3", "enabled": True, "type": "top_hits", "params": {
                            "field": "payload.num_comments", "aggregate": "max", "size": 1
                        }, "schema": "metric"},
                        {"id": "4", "enabled": True, "type": "top_hits", "params": {
                            "field": "payload.subreddit", "aggregate": "concat", "size": 1
                        }, "schema": "metric"}
                    ],
                    "params": {
                        "perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False, "showTotal": False,
                        "totalFunc": "sum", "percentageCol": ""
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Posts ranked by score showing comments correlation",
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                    "index": "reddit_submissions", "query": {"query": "", "language": "kuery"}, "filter": []})}
            }
        }
        self.create_or_update("visualization", "reddit-comments-score", viz_comments_vs_score)
        print("✓ Comments vs Score Analysis")
        
        # 3. Engagement Rate Over Time
        viz_engagement = {
            "attributes": {
                "title": "Engagement Rate Over Time",
                "visState": json.dumps({
                    "title": "Engagement Rate Over Time",
                    "type": "line",
                    "aggs": [
                        {"id": "1", "enabled": True, "type": "avg", "params": {"field": "payload.score"}, "schema": "metric"},
                        {"id": "2", "enabled": True, "type": "avg", "params": {"field": "payload.num_comments"}, "schema": "metric"},
                        {"id": "3", "enabled": True, "type": "date_histogram", "params": {
                            "field": "payload.created_utc", "interval": "auto", "min_doc_count": 1
                        }, "schema": "segment"}
                    ],
                    "params": {
                        "type": "line", "grid": {"categoryLines": False},
                        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom", "show": True}],
                        "valueAxes": [{"id": "ValueAxis-1", "type": "value", "position": "left", "show": True}],
                        "seriesParams": [
                            {"show": True, "type": "line", "mode": "normal", "data": {"label": "Avg Score", "id": "1"}, 
                             "valueAxis": "ValueAxis-1", "lineWidth": 2},
                            {"show": True, "type": "line", "mode": "normal", "data": {"label": "Avg Comments", "id": "2"}, 
                             "valueAxis": "ValueAxis-1", "lineWidth": 2}
                        ],
                        "addTooltip": True, "addLegend": True, "legendPosition": "right"
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Average engagement metrics over time",
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                    "index": "reddit_submissions", "query": {"query": "", "language": "kuery"}, "filter": []})}
            }
        }
        self.create_or_update("visualization", "reddit-engagement", viz_engagement)
        print("✓ Engagement Rate Over Time")
        
        # 4. Top Posts Table
        viz_top_posts = {
            "attributes": {
                "title": "Top Posts Table",
                "visState": json.dumps({
                    "title": "Top Posts Table",
                    "type": "table",
                    "aggs": [
                        {"id": "1", "enabled": True, "type": "terms", "params": {
                            "field": "payload.title.keyword", "orderBy": "2", "order": "desc", "size": 20
                        }, "schema": "bucket"},
                        {"id": "2", "enabled": True, "type": "max", "params": {"field": "payload.score"}, "schema": "metric"},
                        {"id": "3", "enabled": True, "type": "max", "params": {"field": "payload.num_comments"}, "schema": "metric"},
                        {"id": "4", "enabled": True, "type": "terms", "params": {
                            "field": "payload.subreddit", "orderBy": "2", "order": "desc", "size": 1
                        }, "schema": "bucket"},
                        {"id": "5", "enabled": True, "type": "terms", "params": {
                            "field": "payload.author", "orderBy": "2", "order": "desc", "size": 1
                        }, "schema": "bucket"}
                    ],
                    "params": {"perPage": 10, "showPartialRows": False, "showTotal": False}
                }),
                "uiStateJSON": "{}",
                "description": "Top performing posts with details",
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                    "index": "reddit_submissions", "query": {"query": "", "language": "kuery"}, "filter": []})}
            }
        }
        self.create_or_update("visualization", "reddit-top-posts", viz_top_posts)
        print("✓ Top Posts Table")
        
        # 5. Keyword Trend Timeline
        viz_keyword_timeline = {
            "attributes": {
                "title": "Keyword Trends Timeline",
                "visState": json.dumps({
                    "title": "Keyword Trends Timeline",
                    "type": "area",
                    "aggs": [
                        {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
                        {"id": "2", "enabled": True, "type": "date_histogram", "params": {
                            "field": "payload.created_utc", "interval": "auto", "min_doc_count": 1
                        }, "schema": "segment"},
                        {"id": "3", "enabled": True, "type": "terms", "params": {
                            "field": "keywords", "orderBy": "1", "order": "desc", "size": 5
                        }, "schema": "group"}
                    ],
                    "params": {
                        "type": "area", "addTooltip": True, "addLegend": True, "legendPosition": "right",
                        "seriesParams": [{"show": True, "type": "area", "mode": "stacked", "data": {"label": "Count", "id": "1"}}]
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Top keywords trending over time",
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                    "index": "reddit_submissions", "query": {"query": "", "language": "kuery"}, "filter": []})}
            }
        }
        self.create_or_update("visualization", "reddit-keyword-timeline", viz_keyword_timeline)
        print("✓ Keyword Trend Timeline")
        
        # 6. Posting Velocity
        viz_velocity = {
            "attributes": {
                "title": "Posting Velocity (per minute)",
                "visState": json.dumps({
                    "title": "Posting Velocity",
                    "type": "line",
                    "aggs": [
                        {"id": "1", "enabled": True, "type": "count", "params": {}, "schema": "metric"},
                        {"id": "2", "enabled": True, "type": "date_histogram", "params": {
                            "field": "payload.created_utc", "interval": "1m", "min_doc_count": 0
                        }, "schema": "segment"}
                    ],
                    "params": {
                        "type": "line", "addTooltip": True, "addLegend": True, "addTimeMarker": True,
                        "seriesParams": [{"show": True, "type": "line", "mode": "normal", "lineWidth": 2, 
                                        "data": {"label": "Posts/min", "id": "1"}}]
                    }
                }),
                "uiStateJSON": "{}",
                "description": "Real-time posting rate",
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                    "index": "reddit_submissions", "query": {"query": "", "language": "kuery"}, "filter": []})}
            }
        }
        self.create_or_update("visualization", "reddit-velocity", viz_velocity)
        print("✓ Posting Velocity")

    def create_dashboard(self):
        """Create enhanced dashboard with all visualizations."""
        panels = [
            # Row 1: Timeline and Subreddits
            {"version": "8.13.4", "gridData": {"x": 0, "y": 0, "w": 24, "h": 8, "i": "1"}, 
             "panelIndex": "1", "embeddableConfig": {}, "panelRefName": "panel_1"},
            {"version": "8.13.4", "gridData": {"x": 24, "y": 0, "w": 24, "h": 8, "i": "2"}, 
             "panelIndex": "2", "embeddableConfig": {}, "panelRefName": "panel_2"},
            
            # Row 2: Metrics
            {"version": "8.13.4", "gridData": {"x": 0, "y": 8, "w": 12, "h": 6, "i": "3"}, 
             "panelIndex": "3", "embeddableConfig": {}, "panelRefName": "panel_3"},
            {"version": "8.13.4", "gridData": {"x": 12, "y": 8, "w": 12, "h": 6, "i": "4"}, 
             "panelIndex": "4", "embeddableConfig": {}, "panelRefName": "panel_4"},
            
            # Row 3: Keywords and Heatmap
            {"version": "8.13.4", "gridData": {"x": 24, "y": 8, "w": 24, "h": 14, "i": "5"}, 
             "panelIndex": "5", "embeddableConfig": {}, "panelRefName": "panel_5"},
            {"version": "8.13.4", "gridData": {"x": 0, "y": 14, "w": 24, "h": 8, "i": "6"}, 
             "panelIndex": "6", "embeddableConfig": {}, "panelRefName": "panel_6"},
            
            # NEW PANELS - Row 4
            {"version": "8.13.4", "gridData": {"x": 0, "y": 22, "w": 24, "h": 8, "i": "7"}, 
             "panelIndex": "7", "embeddableConfig": {}, "panelRefName": "panel_7"},
            {"version": "8.13.4", "gridData": {"x": 24, "y": 22, "w": 24, "h": 8, "i": "8"}, 
             "panelIndex": "8", "embeddableConfig": {}, "panelRefName": "panel_8"},
            
            # NEW PANELS - Row 5
            {"version": "8.13.4", "gridData": {"x": 0, "y": 30, "w": 16, "h": 8, "i": "9"}, 
             "panelIndex": "9", "embeddableConfig": {}, "panelRefName": "panel_9"},
            {"version": "8.13.4", "gridData": {"x": 16, "y": 30, "w": 16, "h": 8, "i": "10"}, 
             "panelIndex": "10", "embeddableConfig": {}, "panelRefName": "panel_10"},
            {"version": "8.13.4", "gridData": {"x": 32, "y": 30, "w": 16, "h": 8, "i": "11"}, 
             "panelIndex": "11", "embeddableConfig": {}, "panelRefName": "panel_11"},
            
            # NEW PANEL - Row 6: Top Posts Table
            {"version": "8.13.4", "gridData": {"x": 0, "y": 38, "w": 48, "h": 10, "i": "12"}, 
             "panelIndex": "12", "embeddableConfig": {}, "panelRefName": "panel_12"}
        ]
        
        references = [
            {"name": "panel_1", "type": "visualization", "id": "reddit-posts-timeline"},
            {"name": "panel_2", "type": "visualization", "id": "reddit-top-subreddits"},
            {"name": "panel_3", "type": "visualization", "id": "reddit-total-posts"},
            {"name": "panel_4", "type": "visualization", "id": "reddit-avg-score"},
            {"name": "panel_5", "type": "visualization", "id": "reddit-keywords"},
            {"name": "panel_6", "type": "visualization", "id": "reddit-heatmap"},
            # NEW REFERENCES
            {"name": "panel_7", "type": "visualization", "id": "reddit-score-dist"},
            {"name": "panel_8", "type": "visualization", "id": "reddit-engagement"},
            {"name": "panel_9", "type": "visualization", "id": "reddit-keyword-timeline"},
            {"name": "panel_10", "type": "visualization", "id": "reddit-velocity"},
            {"name": "panel_11", "type": "visualization", "id": "reddit-comments-score"},
            {"name": "panel_12", "type": "visualization", "id": "reddit-top-posts"}
        ]
        
        dashboard = {
            "attributes": {
                "title": "Reddit Analytics Dashboard - Enhanced",
                "description": "Real-time Reddit post analytics with advanced metrics",
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
            print("\n✅ Reddit Analytics Dashboard - Enhanced")
            return True
        return False
    

if __name__ == "__main__":
    setup = KibanaVisualization()
    
    print("=" * 50)
    print("KIBANA DASHBOARD SETUP")
    print("=" * 50)
    
    setup.create_index_pattern()
    time.sleep(2)
    setup.create_visualizations()
    time.sleep(2)
    setup.create_dashboard()
    
    print("\n" + "=" * 50)
    print(f"✅ Created {len(setup.created_objects)} objects")
    print("=" * 50)
    print("\n🌐 Access dashboard at:")
    print(f"   {KIBANA_URL}/app/dashboards")
    print("=" * 50)