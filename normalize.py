#!/usr/bin/env python3

import os
import json
import orjson
import pandas as pd
from xopen import xopen
import concurrent.futures
from multiprocessing import Manager, Pool

def chunks(combined_file, chunk_size = 10000):
    with open(combined_file, 'r', encoding = 'utf-8') as f:
        chunk = []
        for line in f:
            chunk.append(line)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def normalize(args):
    chunk, lock, header_written  = args
    output_dir = r"/scratch/ptolemy/users/cc3886/project_cc/complete.csv"

    all_cols =['contributors','coordinates','coordinates.coordinates','coordinates.type','created_at','entities.hashtags','entities.media','entities.symbols','entities.trends','entities.urls','entities.user_mentions','extended_entities.media','favorite_count','favorited','filter_level','geo','geo.coordinates','geo.type','id','id_str','in_reply_to_screen_name','in_reply_to_status_id','in_reply_to_status_id_str','in_reply_to_user_id','in_reply_to_user_id_str','lang','place','place.attributes.street_address','place.bounding_box.coordinates','place.bounding_box.type','place.country','place.country_code','place.full_name','place.id', 'place.name', 'place.place_type','place.url', 'possibly_sensitive','retweet_count', 'retweeted','scopes.place_ids', 'source', 'text', 'timestamp_ms', 'truncated','user.contributors_enabled', 'user.created_at', 'user.default_profile', 'user.default_profile_image', 'user.description','user.favourites_count','user.follow_request_sent', 'user.followers_count', 'user.following', 'user.friends_count', 'user.geo_enabled', 'user.id', 'user.id_str', 'user.is_translator', 'user.lang', 'user.listed_count','user.location', 'user.name', 'user.notifications','user.profile_background_color','user.profile_background_image_url','user.profile_background_image_url_https','user.profile_background_tile','user.profile_banner_url','user.profile_image_url','user.profile_image_url_https','user.profile_link_color','user.profile_sidebar_border_color','user.profile_sidebar_fill_color','user.profile_text_color','user.profile_use_background_image','user.protected','user.screen_name','user.statuses_count','user.time_zone','user.url','user.utc_offset','user.verified']
    batch = []
    for line in chunk:
        batch.append(orjson.loads(line.strip()))
    if not batch:
        return

    df = pd.json_normalize(batch).reindex(columns=all_cols)

    with lock:
        df.to_csv(output_dir, mode='a', index=False, header=not header_written.value)
        header_written.value = True

if __name__ ==  '__main__':

    combined_file  = r"/scratch/ptolemy/users/cc3886/project_cc/combined.ndjson"


    with Manager() as manager:
        lock = manager.Lock()
        header_written = manager.Value('b', False)
        print("okay...")
        with Pool() as p:
            for chunk in chunks(combined_file):
                args = (chunk, lock, header_written)
                p.map(normalize, [args])
