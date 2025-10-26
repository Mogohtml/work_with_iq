import os
import vk_api
from dotenv import load_dotenv
import csv
from typing import List, Union, Dict, Any

from twocaptcha import TwoCaptcha

POST_RECORDS = 100
COMMENT_RECORDS = 100






# Авторизация VK Api
load_dotenv()
session = vk_api.VkApi(login=os.getenv("LOGIN"), password=os.getenv("PASSWORD"))
session.auth()
print("Авторизация прошла успешно!")

vk = session.get_api()


# Выгрузка данных в таблицу
def data_upload(file_name: str, data: List[Dict[str, Any]]) -> int:
    old_data = []
    with open(file_name, "r", newline="", encoding="utf-8") as csv_file:
        csv_file.readline()
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            old_data.append(int(row[1]))

    with open(file_name, "a", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=",")
        cnt = 0
        for post in data:
            for key, value in post.items():
                for comment in value:
                    if comment["id"] in old_data:
                        continue
                    csv_writer.writerow([key, comment["id"], comment["text"]])
                    cnt += 1
    return cnt


# Получение информации постов по айди группы или короткому имени
def get_posts(group: Union[int, str], offset: int) -> Dict[str, Any]:
    if type(group) is str:
        response = vk.wall.get(domain=group, count=POST_RECORDS, offset=offset)
    else:
        response = vk.wall.get(owner_id=group, count=POST_RECORDS, offset=offset)
    return {x["id"]: {"group_id": x["owner_id"]} for x in response["items"]}


# Получение информации по комментариям
def get_comments(posts: Dict[str, Any]) -> List[Dict[str, Any]]:
    comments = []
    for key, value in posts.items():
        response = vk.wall.getComments(owner_id=value["group_id"], post_id=key, count=COMMENT_RECORDS)["items"]
        response = {key: list(
            filter(
                lambda verifiable: verifiable["text"].strip() != "",
                map(
                    lambda comment: {
                        "id": comment["id"],
                        "text": comment["text"].replace("\n", " ").strip()
                    }, response
                ))
        )}
        comments.append(response)
    return comments


if __name__ == "__main__":
    print("[INFO] СКРИПТ ЗАПУЩЕН ...")
    while True:
        input_text = input("Введите айди группы или его короткое имя: ")
        if not input_text.strip():
            print("[INFO] ВЫКЛЮЧЕНИЕ ...")
            break
        idd = input_text
        if input_text.isdecimal():
            idd = int(input_text)

        input_pages = int(input("Введите количество страниц: "))

        comments = []
        posts_cnt = 0
        for i in range(input_pages):
            posts = get_posts(idd, i * POST_RECORDS)
            posts_cnt += len(posts)
            comments += get_comments(posts)
            print(f"[INFO] ЗАГРУЖЕНА СТРАНИЦА: {i+1}/{input_pages}")

        print(f"[INFO] ПОЛУЧЕНО ПОСТОВ: {posts_cnt}")
        cnt = data_upload("data.csv", comments)
        print(f"[INFO] ЗАНЕСЕНО В ТАБЛИЦУ {cnt} КОММЕНТАРИЕВ")
