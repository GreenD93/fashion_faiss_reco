import time
import glob
import os

import asyncio
import aiohttp

import pandas as pd
import numpy as np

from PIL import Image
from io import BytesIO


SLEEP_TIME = 1

# 코루틴으로 이미지 다운로드 받기
# https://sjquant.tistory.com/14
# await : 병목이 발생해서 다른 작업을 통제권을 넘겨줄 필요가 있는 부분에서는 await을 써줌
# await 뒤에 오는 함수도 코루틴으로 작성되어야 함.
async def fetch(sess, src, dst_folder_path):

    img_name = src.split('/')[-1]
    dst = '{0}/{1}'.format(dst_folder_path, img_name)

    async with sess.get(src) as response:
        if response.status != 200:
            response.raise_for_status()

        byte_data = await response.read()

        im_src = Image.open(BytesIO(byte_data))
        im_src.save(dst)


async def fetch_all(sess, chunk, dst_folder_path):
    # await : 코루틴을 실행하는 예약어.
    await asyncio.gather(*[asyncio.create_task(fetch(sess, img_url, dst_folder_path)) for img_url in chunk])

file_name = '20200519_omius_이미지데이터.xlsx'
dst_folder_path = 'dataset'

df = pd.read_excel(file_name)
print(df['imageUrl'].iloc[0])



img_paths = df['imageUrl'].values.tolist()

# n_chunk data
arr_chunks = np.array_split(img_paths, 10000)

# download img
for chunk in arr_chunks:
    async with aiohttp.ClientSession() as sess:
        await fetch_all(sess, chunk, dst_folder_path)

    time.sleep(SLEEP_TIME)

# 불량 이미지들이 존재..!
img_paths = glob.glob('dataset/*.jpg')
for img_path in img_paths:
    try:
        Image.open(img_path)
    except:
        os.remove(img_path)