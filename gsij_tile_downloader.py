#!/usr/bin/env python
# coding: UTF-8


import csv
import datetime
import gzip
import glob
import hashlib
import os
import queue
import re
from threading import Thread

from PIL import Image
import requests


class Mokuroku:
    def __init__(self, type="std", force_download=True):
        self.type = type

        tmp_dir = os.path.join(type, "tmp")
        self.mokuroku_path = os.path.join(tmp_dir, "mokuroku.csv.gz")

        if force_download or not os.path.isfile(self.mokuroku_path):
            mokuroku_url = f"https://cyberjapandata.gsi.go.jp/xyz/{type}/mokuroku.csv.gz"
            download(mokuroku_url, self.mokuroku_path, overwrite=True)

        self.f = gzip.open(self.mokuroku_path, "rt")
        self.reader = csv.reader(self.f)

    def __del__(self):
        self.f.close()

    @classmethod
    def remove_file(cls, type="std"):
        tmp_dir = os.path.join(type, "tmp")
        mokuroku_path = os.path.join(tmp_dir, "mokuroku.csv.gz")
        if os.path.isfile(mokuroku_path):
            os.remove(mokuroku_path)


class Nippo:
    def __init__(self, date: datetime.date, type="std", force_download=True):
        self.date = date
        self.type = type
        self.f = None
        self.reader = []

        tmp_dir = os.path.join(type, "tmp")

        yyyymmdd = "{:04d}{:02d}{:02d}".format(
            self.date.year, self.date.month, self.date.day)

        self.nippo_path = os.path.join(tmp_dir, f"{yyyymmdd}-nippo.csv.gz")

        if force_download or not os.path.isfile(self.nippo_path):
            nippo_url = f"https://cyberjapandata.gsi.go.jp/nippo/{yyyymmdd}-nippo.csv.gz"
            download(nippo_url, self.nippo_path, overwrite=True)

        if os.path.isfile(self.nippo_path):
            self.f = gzip.open(self.nippo_path, "rt")
            self.reader = csv.reader(self.f)

    def __del__(self):
        if self.f:
            self.f.close()


class NippoManager:
    def __init__(self, type="std", force_download=True):
        self.type = type
        self.force_download = force_download

    def get_latest_nippo_dates(self):
        date_list = []

        today = datetime.date.today()
        last_month = (today.month + 11 - 1) % 12 + 1
        day_from = datetime.date(today.year, last_month, 1)
        days = (today - day_from).days

        for i in range(days + 1):
            date = day_from + i * datetime.timedelta(days=1.0)
            date_list.append(date)

        return date_list

    def get_merged_latest_nippo_dict(self):
        nippo_dict = {}
        pattern_type = re.compile("^[^0-9/]+")
        pattern_path = re.compile("[0-9]+/[0-9]+/[0-9]+.png")

        for date in self.get_latest_nippo_dates():
            nippo = Nippo(date, type=self.type,
                          force_download=self.force_download)

            for ary in nippo.reader:
                path_raw = ary[0]
                md5_hex = ary[3]

                type = re.findall(pattern_type, path_raw)
                path = re.findall(pattern_path, path_raw)

                if len(type) and len(path):
                    type = type[0]
                    path = path[0]
                else:
                    print("Invalid path:", path_raw)
                    continue

                if type == self.type:
                    nippo_dict[path] = md5_hex

        return nippo_dict

    def remove_files(self):
        tmp_dir = os.path.join(self.type, "tmp")

        for p in glob.glob(os.path.join(tmp_dir, "*nippo.csv.gz")):
            os.remove(p)


def gen_gsij_tile_url(type, zoom, tile_x, tile_y):
    return f"https://cyberjapandata.gsi.go.jp/xyz/{type}/{zoom}/{tile_x}/{tile_y}.png"


def prepare_md5_dict(map_dir, verbose=False):
    md5_dict = {}

    path_list = glob.glob(os.path.join(
        map_dir, "**", "*.png"), recursive=True)

    pattern_path = re.compile("[0-9]+/[0-9]+/[0-9]+.png")

    for i, path_raw in enumerate(path_list):
        if verbose:
            print(f"\r  {i+1}", end="")

        path_list = re.findall(pattern_path, path_raw)
        if len(path_list) > 0:
            path = path_list[0]
        else:
            print("  Invalid path:", path_raw)
            continue

        with open(path_raw, "rb") as f:
            digest = hashlib.file_digest(f, "md5")
            md5_dict[path] = digest.hexdigest()

    print("")

    return md5_dict


def download(url, path, overwrite=False):
    if not overwrite and os.path.isfile(path):
        return False

    res = requests.get(url)
    if res.status_code == 200:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(res.content)
        ret = True
    elif res.status_code == 404:
        ret = False
    else:
        print(f"[{res.status_code}] {url}")
        ret = False

    return ret


def download_worker(q: queue.Queue):
    while True:
        try:
            entry = q.get()
        except queue.Empty:
            break

        try:
            url, tile_path = entry
            download(url, tile_path, overwrite=True)
        finally:
            q.task_done()


def prepare_queue_and_worker_threads(func, n_workers):
    workers_list = []
    shared_queue = queue.Queue()

    for i in range(n_workers):
        th = Thread(target=func,
                    args=(shared_queue,),
                    daemon=True)
        th.start()
        workers_list.append(th)

    return shared_queue, workers_list


def download_gsij_tile(type: str = "std",
                       target_zoom_levels=[8, 12, 14],
                       force_download=True,
                       n_download_workers=10,
                       eps=1.0e-14):
    target_zoom_levels = list(map(lambda x: str(x), target_zoom_levels))
    download_png_path_list = []

    map_dir = str(type)
    tmp_dir = os.path.join(map_dir, "tmp")

    os.makedirs(tmp_dir, exist_ok=True)

    # Prepare md5 hash of local files
    print("Checking md5 of local files")
    md5_local = prepare_md5_dict(map_dir, verbose=True)

    # Prepare merged latest nippo
    print("Preparing nippo files")
    nippo_manager = NippoManager(type, force_download)
    print("Merging nippo files")
    date_list = nippo_manager.get_latest_nippo_dates()
    print(f"  from {date_list[0]} to {date_list[-1]}")
    nippo = nippo_manager.get_merged_latest_nippo_dict()

    # Prepare mokuroku and update it with nippo
    print("Preparing mokuroku")
    mokuroku = Mokuroku(type, force_download)

    # List tiles to download
    print("Checking mokuroku and find files to download")
    n_files_to_download = 0
    download_queue, workers_list = prepare_queue_and_worker_threads(
        download_worker, n_download_workers)

    for i, entry in enumerate(mokuroku.reader):
        msg = "\r"
        msg += "Downloaded / Download required / mokuroku = "
        msg += "{:d} / {:d} / {:d}".format(
            download_queue.qsize(),
            n_files_to_download,
            i+1)
        msg += " " * 10
        print(msg, end="")

        path, u_time, size, md5 = entry
        zoom, x, y = os.path.splitext(path)[0].split("/")

        if zoom not in target_zoom_levels:
            continue

        if path in nippo:
            md5 = nippo[path]

        # Check local cache
        if path in md5_local and md5_local[path] == md5:
            continue

        url = gen_gsij_tile_url(type, zoom, x, y)
        tile_path = os.path.join(map_dir, path)
        download_png_path_list.append(tile_path)
        download_queue.put([url, tile_path])
        n_files_to_download += 1

    print("")
    print("Finished mokuroku checking")

    while True:
        n_rest = download_queue.qsize()
        pct = 100.0 * n_rest / (n_files_to_download + eps)

        print("\rDownloading {:d} / {:d} ({:.1f}%)".format(
            n_rest,
            n_files_to_download,
            pct), end="")

        if download_queue.qsize() == 0:
            break
    print("")

    # Wait finishing the last download
    download_queue.join()

    return download_png_path_list


def gen_jpg_path_from_png(png_path, pattern=re.compile("[0-9]+/[0-9]+/[0-9]+.png")):
    match_list = re.findall(pattern, png_path)

    if len(match_list) == 0:
        return None

    return os.path.join(os.path.splitext(match_list[0])[0] + ".jpg")


def conv_map_png2jpg(type="std", png_path_list=[], jpg_quality=75):
    src_dir = str(type)
    tgt_dir = src_dir.rstrip("/") + "_jpg"

    job_dict = {}

    # Prepare job dict
    # Files not converted to jpeg
    for path_raw in glob.glob(os.path.join(
            src_dir, "**", "*.png"), recursive=True):
        jpg_path = gen_jpg_path_from_png(path_raw)

        if jpg_path is None:
            print("Invalid path:", path_raw)
            continue

        tgt_path = os.path.join(tgt_dir, jpg_path)

        if not os.path.exists(tgt_path):
            job_dict[path_raw] = tgt_path

    # Files must be converted to jpeg
    for png_path in png_path_list:
        jpg_path = gen_jpg_path_from_png(png_path)

        if jpg_path is None:
            print("Invalid path:", path_raw)
            continue

        tgt_path = os.path.join(tgt_dir, jpg_path)

        job_dict[path_raw] = tgt_path

    # Convert png to jpeg
    for i, (src_path, tgt_path) in enumerate(job_dict.items()):
        print(f"\r  {i+1:d} / {len(job_dict):d}", end="")

        if not os.path.isfile(src_path):
            print("File not found:", src_path)
            continue

        img = Image.open(src_path).convert("RGB")

        os.makedirs(os.path.dirname(tgt_path), exist_ok=True)

        img.save(tgt_path, quality=jpg_quality)
    print("")


def main(type_list=["std"],
         target_zoom_levels=[8, 12, 14],
         force_download=False,
         n_download_workers=10,
         remove_mokuroku=False,
         remove_nippo=False,
         conv_to_jpeg=True,
         jpg_quality=75):
    for type in type_list:
        print("Map type:", type)

        download_png_path_list = download_gsij_tile(type,
                                                    target_zoom_levels,
                                                    force_download,
                                                    n_download_workers)

        if remove_mokuroku:
            Mokuroku.remove_file(type)

        if remove_nippo:
            nm = NippoManager(type)
            nm.remove_files()

        if conv_to_jpeg:
            print("Converting png to jpg")
            conv_map_png2jpg(type, download_png_path_list, jpg_quality)

        print("Done", type)


if __name__ == '__main__':
    main()
