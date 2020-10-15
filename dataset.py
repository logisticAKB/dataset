import argparse
import logging
import pandas as pd
import time
import signal
import youtube_dl
import os
import threading

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S")

parser = argparse.ArgumentParser()
parser.add_argument("--work_path", required=True)
parser.add_argument("--csv_path", required=True)
parser.add_argument("--n_threads", type=int, choices=range(2, 20), required=True)

args = parser.parse_args()

n_working_threads = 0
n_downloaded = 0
threads = []


class Logger(object):
    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


def thread_cleanup():
    for t in threads:
        if t.is_alive():
            t.join()


def downloader(i, video_name, n):
    global n_working_threads
    n_working_threads += 1

    try:
        ydl_opts = {
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio',
            'merge-output-format': 'mp4',
            'prefer-ffmpeg': True,
            'nooverwrites': True,
            'logger': Logger(),
            'outtmpl': os.path.join(args.work_path, video_name),
        }
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f'https://www.youtube.com/watch?v={video_name}'])

        global n_downloaded
        n_downloaded += 1
        logging.info(f'{video_name} downloaded ({n_downloaded}/{n})')
    except Exception as e:
        logging.info(e)

    n_working_threads -= 1


def start_thread(i, video_name, n):
    global n_working_threads
    while n_working_threads >= args.n_threads:
        pass
    logging.info(f'Downloading {video_name}')
    t = threading.Thread(target=downloader, args=(i, video_name, n), daemon=True)
    threads.append(t)
    t.start()


def main():
    logging.info(f'Started downloading with {args.n_threads} threads')
    data = pd.read_csv(args.csv_path)

    for i, row in data.iterrows():
        video_name = row.YouTube_ID
        start_thread(i, video_name, data.shape[0])

    thread_cleanup()
    logging.info(f'Downloaded {n_downloaded} files')


if __name__ == '__main__':
    main()
